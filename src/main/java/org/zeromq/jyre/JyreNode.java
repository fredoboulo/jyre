package org.zeromq.jyre;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZBeacon;
import org.zeromq.ZCert;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;
import org.zeromq.ZPoller;
import org.zeromq.ZPoller.EventsHandler;
import org.zeromq.jyre.Zre.Hello;
import org.zeromq.timer.ZTicker;
import org.zeromq.zgossip.ZGossip;
import org.zeromq.zproto.annotation.actor.Actor;
import org.zeromq.zproto.annotation.actor.Command;
import org.zeromq.zproto.annotation.actor.Endpoint;
import org.zeromq.zproto.annotation.actor.Message;
import org.zeromq.zproto.annotation.actor.Pipe;
import org.zeromq.zproto.annotation.actor.Receive;
import org.zeromq.zproto.annotation.actor.Ticker;

@Actor("JyreAgentActor")
public class JyreNode
{
    private static final Logger log = LoggerFactory.getLogger(JyreNode.class);

    private static final int PING_PORT_NUMBER = 9991;
    public static final int PING_INTERVAL    = 1000; //  Once per second

    //  =====================================================================
    //  Asynchronous part, works in the background

    //  Beacon frame has this format:
    //
    //  Z R E       3 bytes
    //  version     1 byte, %x01
    //  UUID        16 bytes
    //  port        2 bytes in network order

    protected static class Beacon
    {
        static final int BEACON_SIZE = 22;

        static final String BEACON_PROTOCOL = "ZRE";
        static final byte   BEACON_VERSION  = 0x01;

        private final byte[] protocol = BEACON_PROTOCOL.getBytes();
        private final byte   version  = BEACON_VERSION;
        private final UUID   uuid;
        private short        port;

        Beacon(ByteBuffer buffer)
        {
            long msb = buffer.getLong();
            long lsb = buffer.getLong();
            uuid = new UUID(msb, lsb);
            port = (short) (0xffff & buffer.getShort());
        }

        Beacon(UUID uuid, short port)
        {
            this.uuid = uuid;
            this.port = port;
        }

        ByteBuffer getBuffer()
        {
            ByteBuffer buffer = ByteBuffer.allocate(BEACON_SIZE);
            buffer.put(protocol);
            buffer.put(version);
            buffer.putLong(uuid.getMostSignificantBits());
            buffer.putLong(uuid.getLeastSignificantBits());
            buffer.putShort((short) port);
            buffer.flip();
            return buffer;
        }
    }

    private final ZreUdp                udp;
    private final String                host;                           //  Our host IP address
    private final short                 port;                           //  Our inbox port number
    private final UUID                  uuid       = UUID.randomUUID(); //  Our UUID as hex string
    private final String                identity   = uuidStr(uuid);     //  Our UUID as hex string
    private String                      endpoint;                       //  ipaddress:port endpoint
    private byte                        status;                         //  Our own change counter
    private final Map<String, ZrePeer>  peers      = new HashMap<>();   //  Hash of known peers, fast lookup
    private final Map<String, ZreGroup> peerGroups = new HashMap<>();   //  Groups that our peers are in
    private final Map<String, ZreGroup> ownGroups  = new HashMap<>();   //  Groups that we are in
    private final Map<String, String>   headers    = new HashMap<>();   //  Our header values
    private final int                   evasiveAt;
    private final int                   expiredAt;

    private ZMQ.Socket pipe;
    private ZMQ.Socket outbox;

    private boolean terminated;
    private boolean verbose;

    private int beaconPort = 5670;
    private String ephemeralPort;
    private byte beaconVersion = 0x01;
    private ZBeacon beacon;

    private long evasiveTimeout = 5000;
    private long expiredTimeout = 30_000;
    private long interval = 0;
//    private UUID uuid;
    private String name = uuid.toString().substring(0, 6);
    private String advertisedEndpoint;
//    private int port;
//    private byte status;
    private ZGossip gossip;
    private String gossipBind;
    private String gossipConnect;
    private String publicKey;
    private String secretKey;
    private String zapDomain = "global";

    JyreNode(String name, @Endpoint(type = SocketType.ROUTER) ZMQ.Socket inbox,
             @Ticker ZTicker ticker,
             ZContext ctx, ZMQ.Socket pipe,
             ZPoller poller, int pingInterval, int evasiveAt, int expiredAt)
    {
        this.evasiveAt = evasiveAt;
        this.expiredAt = expiredAt;
        Thread.currentThread().setName(name);
        udp = new ZreUdp(PING_PORT_NUMBER);
        int port = inbox.bindToRandomPort("tcp://*", 0xc000, 0xffff);
        if (port < 0) {
            //  Interrupted
            udp.destroy();
            throw new IllegalStateException("Failed to bind a random port");
        }
        this.port = (short) port;
        host = udp.host();
        endpoint = String.format("%s:%d", host, port);
        ticker.addTimer(pingInterval, (args) -> {
            sendBeacon();
            pingAllPeers(pipe);
        });
        EventsHandler handler = new EventsHandler()
        {
            @Override
            public boolean events(SelectableChannel channel, int events)
            {
                recvUdpBeacon(ctx, pipe);
                return true;
            }

            @Override
            public boolean events(Socket socket, int events)
            {
                assert (false);
                return false;
            }
        };
        poller.register(udp.handle(), handler, ZPoller.IN);
    }

    private static String uuidStr(UUID uuid)
    {
        return uuid.toString().replace("-", "").toUpperCase();
    }

    protected void destroy()
    {
        for (ZrePeer peer : peers.values()) {
            peer.destroy();
        }
        for (ZreGroup group : peerGroups.values()) {
            group.destroy();
        }
        for (ZreGroup group : ownGroups.values()) {
            group.destroy();
        }

        udp.destroy();
    }

    private void gossipStart(ZContext ctx)
    {
        if (gossip == null) {
            beaconPort = 0;
            gossip = new ZGossip(ctx, name, null);
            gossip.verbose(verbose);
        }
    }

    private void nodeStart(ZContext ctx, ZPoller poller, ZMQ.Socket inbox)
    {
        if (secretKey != null) {
            if (verbose) {
                log.debug("applying cert to inbox {}", inbox);
            }
            ZCert cert = new ZCert(); // TODO cert with public and private keys
            cert.apply(inbox);
            inbox.setCurveServer(true);
            inbox.setZAPDomain(zapDomain);
        }
        if (beaconPort > 0) {
            //  Start beacon discovery
            if (secretKey != null) {
                // upgrade the beacon version
                if (verbose) {
                    log.debug("switching to beacon v3");
                }
                beaconVersion = 0x3;
            }
            // TODO instantiate beacon
        }
        else {
            //  Start gossip discovery
            gossip = new ZGossip(ctx, name, null); // TODO cert
            //  If application didn't set an endpoint explicitly, grab ephemeral
            //  port on all available network interfaces.
            if (endpoint == null) {
                gossip.bind("tcp://*:*");
                endpoint = gossip.lastEndpoint();
            } else {
                gossip.bind(endpoint); // TODO check result of bind
            }
            String publishedEndpoint = endpoint;
            if (advertisedEndpoint != null) {
                // if the advertised endpoint is set and different than our bound endpoint
                publishedEndpoint = advertisedEndpoint;
            }
            // further arrange if we have a public key associated
            if (publicKey != null) {
                publishedEndpoint = publishedEndpoint + "|" + publicKey;
            }
            gossip.publish(uuid(), publishedEndpoint);
            //  Start polling on zgossip
            poller.register(gossip.agent().pipe(), ZPoller.IN);
        }
    }

    //  Stop node discovery and interconnection
    //  TODO: clear peer tables; test stop/start cycles; how will this work
    //  with gossip network? Do we leave that running in the meantime?
    private void nodeStop()
    {

    }

    private byte incStatus()
    {
        return ++status;
    }

    //  Delete peer for a given endpoint
    private void purgePeer(String endpoint)
    {
        for (Map.Entry<String, ZrePeer> entry : peers.entrySet()) {
            ZrePeer peer = entry.getValue();
            if (peer.endpoint().equals(endpoint)) {
                peer.disconnect();
            }
        }
    }

    //  Find or create peer via its UUID string
    private ZrePeer requirePeer(ZContext ctx, String identity, String address, short port, ZMQ.Socket pipe)
    {
        ZrePeer peer = peers.get(identity);
        if (peer == null) {
            //  Purge any previous peer on same endpoint
            String endpoint = String.format("%s:%d", address, port & 0xffff);

            purgePeer(this.endpoint);

            peer = ZrePeer.newPeer(identity, peers, ctx);
            peer.connect(this.identity, endpoint);

            //  Handshake discovery by sending HELLO as first message
            ZreMsg msg = ZreMsg.newHello(null);
            Zre.Hello hello = msg.hello();
//            hello.ipaddress = this.udp.host();
//            hello.mailbox = this.port;
            hello.groups = new ArrayList<>(ownGroups.keySet());
            hello.status = status;
            hello.headers = new HashMap<>(headers);
            peer.send(msg);

            //  Now tell the caller about the peer
            pipe.sendMore("ENTER");
            pipe.send(identity);
        }
        return peer;
    }

    //  Find or create group via its name
    private ZreGroup requirePeerGroup(String name)
    {
        ZreGroup group = peerGroups.get(name);
        if (group == null) {
            group = ZreGroup.newGroup(name, peerGroups);
        }
        return group;

    }

    private ZreGroup joinPeerGroup(ZrePeer peer, String name, ZMQ.Socket pipe)
    {
        ZreGroup group = requirePeerGroup(name);
        group.join(peer);

        //  Now tell the caller about the peer joined a group
        pipe.sendMore("JOIN");
        pipe.sendMore(peer.identity());
        pipe.send(name);

        return group;
    }

    private ZreGroup leavePeerGroup(ZrePeer peer, String name, ZMQ.Socket pipe)
    {
        ZreGroup group = requirePeerGroup(name);
        group.leave(peer);

        //  Now tell the caller about the peer joined a group
        pipe.sendMore("LEAVE");
        pipe.sendMore(peer.identity());
        pipe.send(name);

        return group;
    }

    @Command
    String uuid()
    {
        return uuid.toString().replaceAll("-", "");
    }

    @Command
    String name()
    {
        return name;
    }

    @Command
    void setName(String name)
    {
        this.name = name;
    }

    @Command
    void setVerbose(boolean verbose)
    {
        this.verbose = verbose;
    }

    @Command(control = true)
    boolean terminate()
    {
        return false;
    }

    @Command
    void setPort(int port)
    {
        beaconPort = port;
    }

    @Command
    void setBeaconPeerPort(String port)
    {
        ephemeralPort = port;
    }

    @Command
    void setEvasiveTimeout(long interval)
    {
        evasiveTimeout = interval;
    }

    @Command
    void setSilentTimeout(long interval)
    {
        evasiveTimeout = interval;
    }

    @Command
    void setExpiredTimeout(long interval)
    {
        expiredTimeout = interval;
    }

    @Command
    void setInterval(long interval)
    {
        this.interval = interval;
    }

    @Command
    void setInterface(String value)
    {
        // TODO
    }

    @Command
    boolean setEndpoint(String value)
    {
        // TODO
        return false;
    }

    @Command
    void setContestInGroup(String value)
    {
        // TODO
    }

    @Command
    boolean start(ZContext ctx, ZPoller poller, ZMQ.Socket inbox)
    {
        nodeStart(ctx, poller, inbox);
        // TODO
        return true;
    }

    @Command
    void stop()
    {
        nodeStop();
    }

    @Command
    ZMsg recv()
    {
        // TODO
        return null;
    }

    @Command
    void whisper(String id, ZMsg request)
    {
        ZrePeer peer = peers.get(id);

        //  Send frame on out to peer's mailbox, drop message
        //  if peer doesn't exist (may have been destroyed)
        if (peer != null) {
            ZreMsg msg = ZreMsg.newWhisper(null);
            Zre.Whisper payload = msg.whisper();
            payload.content = request.pop();
            peer.send(msg);
        }
    }

    @Command
    void shout(String name, ZMsg request)
    {
        //  Get group to send message to
        ZreGroup group = peerGroups.get(name);
        if (group != null) {
            ZreMsg msg = ZreMsg.newShout(null);
            Zre.Shout payload = msg.shout();
            payload.group = name;
            payload.content = request.pop();
            group.send(msg);
        }
    }

    @Command
    void join(String name)
    {
        ZreGroup group = ownGroups.get(name);
        if (group == null) {
            //  Only send if we're not already in group
            group = ZreGroup.newGroup(name, ownGroups);
            ZreMsg msg = ZreMsg.newJoin(null);
            Zre.Join payload = msg.join();
            payload.group = group.name;
            //  Update status before sending command
            payload.status = incStatus();

            sendPeers(msg);
            msg.destroy();
        }
    }

    @Command
    void leave(String name)
    {
        ZreGroup group = ownGroups.get(name);
        if (group != null) {
            //  Only send if we are actually in group
            ZreMsg msg = ZreMsg.newLeave(null);
            Zre.Leave leave = msg.leave();
            leave.group = name;
            //  Update status before sending command
            leave.status = incStatus();
            sendPeers(msg);
            ownGroups.remove(name);
        }
    }

    @Command
    void setHeader(String name, String value)
    {
        headers.put(name, value);
    }

    @Command
    List<String> peers()
    {
        return new ArrayList<>(peers.keySet());
    }

    @Command
    List<String> peersByGroup(String name)
    {
        ZreGroup group = peerGroups.get(name);
        if (group == null) {
            return Collections.emptyList();
        }
        return new ArrayList<>(group.peers());
    }

    @Command
    String peerEndpoint(String id)
    {
        ZrePeer peer = peers.get(id);
        if (peer != null) {
            return peer.endpoint();
        }
        return "";
    }

    @Command
    String peerName(String id)
    {
        ZrePeer peer = peers.get(id);
        if (peer != null) {
            return peer.name();
        }
        return "";
    }

    @Command
    String peerHeader(String id, String header)
    {
        ZrePeer peer = peers.get(id);
        if (peer != null) {
            return peer.header(header, "");
        }
        return "";
    }

    @Command
    List<String> ownGroups()
    {
        return new ArrayList<>(ownGroups.keySet());
    }

    @Command
    List<String> peerGroups()
    {
        return new ArrayList<>(peerGroups.keySet());
    }

    @Receive("inbox")
    void inbox(@Message Zre.Hello msg, ZFrame address, ZContext ctx, ZMQ.Socket pipe)
    {
        ZrePeer peer = findPeer(msg, address, ctx, pipe);
        assert (peer != null);
        //  Join peer to listed groups
        for (String name : msg.groups) {
            joinPeerGroup(peer, name, pipe);
        }
        //  Hello command holds latest status of peer
        peer.setStatus(msg.status);

        //  Store peer headers for future reference
        peer.setHeaders(msg.headers);
    }

    @Receive("inbox")
    void inbox(@Message Zre.Whisper msg, ZFrame address, ZContext ctx, ZMQ.Socket pipe)
    {
        ZrePeer peer = findPeer(msg, address, ctx, pipe);
        if (peer == null) {
            return;
        }
        //  Pass up to caller API as WHISPER event
        ZFrame cookie = msg.content;
        pipe.sendMore("WHISPER");
        address.send(pipe, ZMQ.SNDMORE);
        cookie.send(pipe, 0); // let msg free the frame
        //  Activity from peer resets peer timers
        peer.refresh(evasiveAt, expiredAt);
    }

    @Receive("inbox")
    void inbox(@Message Zre.Shout msg, ZFrame address, ZContext ctx, ZMQ.Socket pipe)
    {
        ZrePeer peer = findPeer(msg, address, ctx, pipe);
        if (peer == null) {
            return;
        }
        //  Pass up to caller as SHOUT event
        ZFrame cookie = msg.content;
        pipe.sendMore("SHOUT");
        address.send(pipe, ZMQ.SNDMORE);
        pipe.sendMore(msg.group);
        cookie.send(pipe, 0); // let msg free the frame
        //  Activity from peer resets peer timers
        peer.refresh(evasiveAt, expiredAt);
    }

    @Receive("inbox")
    void inbox(@Message Zre.Ping msg, ZFrame address, ZContext ctx, ZMQ.Socket pipe)
    {
        ZrePeer peer = findPeer(msg, address, ctx, pipe);
        if (peer == null) {
            return;
        }
        ZreMsg pingOK = ZreMsg.newPingOk(null);
        peer.send(pingOK);
        //  Activity from peer resets peer timers
        peer.refresh(evasiveAt, expiredAt);
    }

    @Receive("inbox")
    void inbox(@Message Zre.Join msg, ZFrame address, ZContext ctx, ZMQ.Socket pipe)
    {
        ZrePeer peer = findPeer(msg, address, ctx, pipe);
        if (peer == null) {
            return;
        }
        joinPeerGroup(peer, msg.group, pipe);
        assert (msg.status == peer.status());
        //  Activity from peer resets peer timers
        peer.refresh(evasiveAt, expiredAt);
    }

    @Receive("inbox")
    void inbox(@Message Zre.Leave msg, ZFrame address, ZContext ctx, ZMQ.Socket pipe)
    {
        ZrePeer peer = findPeer(msg, address, ctx, pipe);
        if (peer == null) {
            return;
        }
        leavePeerGroup(peer, msg.group, pipe);
        assert (msg.status == peer.status());
        //  Activity from peer resets peer timers
        peer.refresh(evasiveAt, expiredAt);
    }

    private ZrePeer findPeer(Zre msg, ZFrame address, ZContext ctx, ZMQ.Socket pipe)
    {
        String identity = new String(address.getData());

        //  On HELLO we may create the peer if it's unknown
        //  On other commands the peer must already exist
        ZrePeer peer = peers.get(identity);
        if (msg instanceof Zre.Hello) {
            Zre.Hello hello = (Hello) msg;
//            peer = requirePeer(ctx, identity, hello.ipaddress, hello.mailbox, pipe);
            assert (peer != null);
            peer.setReady(true);
        }
        //  Ignore command if peer isn't ready
        if (peer == null || !peer.ready()) {
            return null;
        }

        if (!peer.checkMessage(msg)) {
            System.err.printf("W: [%s] lost messages from %s\n", this.identity, identity);
            assert (false);
        }
        return peer;
    }

    //  Handle beacon
    private boolean recvUdpBeacon(ZContext ctx, Socket pipe)
    {
        ByteBuffer buffer = ByteBuffer.allocate(Beacon.BEACON_SIZE);

        //  Get beacon frame from network
        int size = 0;
        try {
            size = udp.recv(buffer);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        buffer.rewind();

        //  Basic validation on the frame
        if (size != Beacon.BEACON_SIZE || buffer.get() != 'Z' || buffer.get() != 'R' || buffer.get() != 'E'
                || buffer.get() != Beacon.BEACON_VERSION) {
            return true; //  Ignore invalid beacons
        }

        //  If we got a UUID and it's not our own beacon, we have a peer
        Beacon beacon = new Beacon(buffer);
        if (!beacon.uuid.equals(uuid)) {
            String identity = uuidStr(beacon.uuid);
            ZrePeer peer = requirePeer(ctx, identity, udp.from(), beacon.port, pipe);
            peer.refresh(evasiveAt, expiredAt);
        }

        return true;
    }

    //  Send moar beacon
    private void sendBeacon()
    {
        Beacon beacon = new Beacon(uuid, port);
        try {
            udp.send(beacon.getBuffer());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    //  We do this once a second:
    //  - if peer has gone quiet, send TCP ping
    //  - if peer has disappeared, expire it
    private void pingAllPeers(ZMQ.Socket pipe)
    {
        Iterator<Map.Entry<String, ZrePeer>> it = peers.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ZrePeer> entry = it.next();
            String identity = entry.getKey();
            ZrePeer peer = entry.getValue();
            if (System.currentTimeMillis() >= peer.expiredAt()) {
                //  If peer has really vanished, expire it
                pipe.sendMore("EXIT");
                pipe.send(identity);
                deletePeerFromGroups(peer);
                it.remove();
                peer.destroy();
            }
            else if (System.currentTimeMillis() >= peer.evasiveAt()) {
                //  If peer is being evasive, force a TCP ping.
                //  TODO: do this only once for a peer in this state;
                //  it would be nicer to use a proper state machine
                //  for peer management.
                ZreMsg msg = ZreMsg.newPing(null);
                peer.send(msg);
            }
        }
    }

    //  Send message to all peers
    private void sendPeers(ZreMsg msg)
    {
        for (ZrePeer peer : peers.values()) {
            peer.send(msg);
        }
    }

    //  Remove peer from group, if it's a member
    private void deletePeerFromGroups(ZrePeer peer)
    {
        for (ZreGroup group : peerGroups.values()) {
            group.leave(peer);
        }
    }
}
