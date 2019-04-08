package org.zeromq.jyre;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;
import org.zeromq.ZPoller;
import org.zeromq.ZPoller.EventsHandler;
import org.zeromq.jyre.Zre.Hello;
import org.zeromq.timer.ZTicker;
import org.zeromq.zproto.annotation.actor.Actor;
import org.zeromq.zproto.annotation.actor.Command;
import org.zeromq.zproto.annotation.actor.Endpoint;
import org.zeromq.zproto.annotation.actor.Message;
import org.zeromq.zproto.annotation.actor.Ticker;

@Actor("Jyre")
public class JyreAgent
{
    public static final int PING_PORT_NUMBER = 9991;
    public static final int UBYTE_MAX        = 0xff;
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
        public static final int BEACON_SIZE = 22;

        public static final String BEACON_PROTOCOL = "ZRE";
        public static final byte   BEACON_VERSION  = 0x01;

        private final byte[] protocol = BEACON_PROTOCOL.getBytes();
        private final byte   version  = BEACON_VERSION;
        private final UUID   uuid;
        private int          port;

        public Beacon(ByteBuffer buffer)
        {
            long msb = buffer.getLong();
            long lsb = buffer.getLong();
            uuid = new UUID(msb, lsb);
            port = buffer.getShort();
            if (port < 0) {
                port = (0xffff) & port;
            }
        }

        public Beacon(UUID uuid, int port)
        {
            this.uuid = uuid;
            this.port = port;
        }

        public ByteBuffer getBuffer()
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
    private final int                   port;                           //  Our inbox port number
    private final UUID                  uuid       = UUID.randomUUID(); //  Our UUID as hex string
    private final String                identity   = uuidStr(uuid);     //  Our UUID as hex string
    private final String                endpoint;                       //  ipaddress:port endpoint
    private int                         status;                         //  Our own change counter
    private final Map<String, ZrePeer>  peers      = new HashMap<>();   //  Hash of known peers, fast lookup
    private final Map<String, ZreGroup> peerGroups = new HashMap<>();   //  Groups that our peers are in
    private final Map<String, ZreGroup> ownGroups  = new HashMap<>();   //  Groups that we are in
    private final Map<String, String>   headers    = new HashMap<>();   //  Our header values
    private final ZTicker               ticker;
    private final int                   evasiveAt;
    private final int                   expiredAt;

    JyreAgent(String name, @Endpoint(type = SocketType.ROUTER) ZMQ.Socket inbox, 
              @Ticker ZTicker ticker,
              ZContext ctx, ZMQ.Socket pipe,
            ZPoller poller, int pingInterval, int evasiveAt, int expiredAt)
    {
        this.evasiveAt = evasiveAt;
        this.expiredAt = expiredAt;
        Thread.currentThread().setName(name);
        udp = new ZreUdp(PING_PORT_NUMBER);
        port = inbox.bindToRandomPort("tcp://*", 0xc000, 0xffff);
        if (port < 0) {
            //  Interrupted
            udp.destroy();
            throw new IllegalStateException("Failed to bind a random port");
        }
        host = udp.host();
        endpoint = String.format("%s:%d", host, port);
        this.ticker = ticker;
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

    private int incStatus()
    {
        if (++status > UBYTE_MAX) {
            status = 0;
        }
        return status;
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
    private ZrePeer requirePeer(ZContext ctx, String identity, String address, int port, ZMQ.Socket pipe)
    {
        ZrePeer peer = peers.get(identity);
        if (peer == null) {
            //  Purge any previous peer on same endpoint
            String endpoint = String.format("%s:%d", address, port);

            purgePeer(this.endpoint);

            peer = ZrePeer.newPeer(identity, peers, ctx);
            peer.connect(this.identity, endpoint);

            //  Handshake discovery by sending HELLO as first message
            ZreMsg msg = ZreMsg.newHello(null);
            Zre.Hello hello = msg.hello();
            hello.ipaddress = this.udp.host();
            hello.mailbox = this.port;
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
    public void whisper(ZMsg request)
    {
        String identity = request.popString();
        ZrePeer peer = peers.get(identity);

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
    public void shout(ZContext ctx, ZMsg request)
    {
        //  Get group to send message to
        String name = request.popString();
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
    void join(String name, ZMQ.Socket pipe)
    {
        ZreGroup group = ownGroups.get(name);
        if (group == null) {
            //  Only send if we're not already in group
            group = ZreGroup.newGroup(name, ownGroups);
            ZreMsg msg = ZreMsg.newJoin(null);
            Zre.Join payload = msg.join();
            payload.group = name;
            //  Update status before sending command
            payload.status = incStatus();

            sendPeers(msg);
            msg.destroy();
        }
    }

    @Command
    void leave(String name, ZMQ.Socket pipe)
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
    public void setHeader(String name, String value)
    {
        headers.put(name, value);
    }

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

    ZrePeer findPeer(Zre msg, ZFrame address, ZContext ctx, ZMQ.Socket pipe)
    {
        String identity = new String(address.getData());

        //  On HELLO we may create the peer if it's unknown
        //  On other commands the peer must already exist
        ZrePeer peer = peers.get(identity);
        if (msg instanceof Zre.Hello) {
            Zre.Hello hello = (Hello) msg;
            peer = requirePeer(ctx, identity, hello.ipaddress, hello.mailbox, pipe);
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
    protected boolean recvUdpBeacon(ZContext ctx, Socket pipe)
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
    public void sendBeacon()
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
    public void pingAllPeers(ZMQ.Socket pipe)
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
                deletePeerFromGroups(peerGroups, peer);
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
    private static void deletePeerFromGroups(Map<String, ZreGroup> groups, ZrePeer peer)
    {
        for (ZreGroup group : groups.values()) {
            group.leave(peer);
        }
    }
}
