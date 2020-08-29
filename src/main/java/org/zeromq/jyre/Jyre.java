package org.zeromq.jyre;

import org.zeromq.ZContext;
import org.zeromq.ZMsg;

import java.util.List;

public class Jyre
{
    private final JyreAgentActor actor;

    Jyre(ZContext ctx, String s, int i, int i2, int i3)
    {
        actor = new JyreAgentActor(ctx, s, i, i2, i3);
    }

    //  Return our node UUID string, after successful initialization
    public String uuid()
    {
        return actor.uuid();
    }

    //  Return our node name, after successful initialization. By default
    //  is taken from the UUID and shortened.
    public String name()
    {
        return actor.name();
    }

    //  Set node header; these are provided to other nodes during discovery
    //  and come in each ENTER message.
    public void setName(String name)
    {
        actor.setName(name);
    }

    //  Set node header; these are provided to other nodes during discovery
    //  and come in each ENTER message.
    public void setHeader(String name, String value)
    {
        actor.setHeader(name, value);
    }

    //  Set verbose mode; this tells the node to log all traffic as well as
    //  all major events.
    public void setVerbose(boolean verbose)
    {
        actor.setVerbose(verbose);
    }

    //  Set UDP beacon discovery port; defaults to 5670, this call overrides
    //  that so you can create independent clusters on the same network, for
    //  e.g. development vs. production. Has no effect after zyre_start().
    public void setPort(int port)
    {
        actor.setPort(port);
    }

    //  Set TCP ephemeral port for beacon; defaults to 0, and the port is random.
    //  This call overrides this to bypass some firewall issues with random ports.
    //  Has no effect after zyre_start().
    public void setBeaconPeerPort(String port)
    {
        actor.setBeaconPeerPort(port);
    }

    //  Set the node evasiveness timeout, in milliseconds. Default is 5000.
    //  This can be tuned in order to deal with expected network conditions
    //  and the response time expected by the application. This is tied to
    //  the beacon interval and rate of messages received.
    public void setEvasiveTimeout(int interval)
    {
        actor.setEvasiveTimeout(interval);
    }

    //  Set the node silence timeout, in milliseconds. Default is 5000.
    //  Silence means that a peer does not send messages and does not
    //  answer to ping. SILENT event is triggered one second after the
    //  configured silence timeout, and every second after that until
    //  the expired timeout is reached.
    //  This can be tuned in order to deal with expected network conditions
    //  and the response time expected by the application. This is tied to
    //  the beacon interval and rate of messages received.
    //  NB: in current implementation, zyre_set_silent_timeout is redundant
    //  with zyre_set_evasive_timeout and calls the same code underneath.
    public void setSilentTimeout(int interval)
    {
        actor.setSilentTimeout(interval);
    }

    //  Set the node expiration timeout, in milliseconds. Default is 30000.
    //  This can be tuned in order to deal with expected network conditions
    //  and the response time expected by the application. This is tied to
    //  the beacon interval and rate of messages received.
    public void setExpiredTimeout(int interval)
    {
        actor.setExpiredTimeout(interval);
    }

    //  Set UDP beacon discovery interval, in milliseconds. Default is instant
    //  beacon exploration followed by pinging every 1,000 msecs.
    public void setInterval(int interval)
    {
        actor.setInterval(interval);
    }

    //  Set network interface for UDP beacons. If you do not set this, CZMQ will
    //  choose an interface for you. On boxes with several interfaces you should
    //  specify which one you want to use, or strange things can happen.
    public void setInterface(String value)
    {
        actor.setInterface(value);
    }

    //  By default, Zyre binds to an ephemeral TCP port and broadcasts the local
    //  host name using UDP beaconing. When you call this method, Zyre will use
    //  gossip discovery instead of UDP beaconing. You MUST set-up the gossip
    //  service separately using zyre_gossip_bind() and _connect(). Note that the
    //  endpoint MUST be valid for both bind and connect operations. You can use
    //  inproc://, ipc://, or tcp:// transports (for tcp://, use an IP address
    //  that is meaningful to remote as well as local nodes). Returns 0 if
    //  the bind was successful, else -1.
    public boolean setEndpoint(String value)
    {
        return actor.setEndpoint(value);
    }

    //  This options enables a peer to actively contest for leadership in the
    //  given group. If this option is not set the peer will still participate in
    //  elections but never gets elected. This ensures that a consent for a leader
    //  is reached within a group even though not every peer is contesting for
    //  leadership.
    public void setContestInGroup(String value)
    {
        actor.setContestInGroup(value);
    }

    // TODO DRAFT API here

    //  Start node, after setting header values. When you start a node it
    //  begins discovery and connection. Returns 0 if OK, -1 if it wasn't
    //  possible to start the node. If you want to use gossip discovery, set
    //  the endpoint (optionally), then bind/connect the gossip network, and
    //  only then start the node.
    public boolean start()
    {
        return actor.start();
    }

    //  Stop node; this signals to other peers that this node will go away.
    //  This is polite; however you can also just destroy the node without
    //  stopping it.
    public void stop()
    {
        actor.stop();
    }

    //  Join a named group; after joining a group you can send messages to
    //  the group and all Zyre nodes in that group will receive them.
    public void join(String name)
    {
        actor.join(name);
    }

    //  Leave a group
    public void leave(String name)
    {
        actor.leave(name);
    }

    //  Receive next message from network; the message may be a control
    //  message (ENTER, EXIT, JOIN, LEAVE) or data (WHISPER, SHOUT).
    //  Returns zmsg_t object, or NULL if interrupted
    public ZMsg recv()
    {
        return actor.recv();
    }

    //  Send message to single peer, specified as a UUID string
    public void whisper(String peer, ZMsg request)
    {
        actor.whisper(peer, request);
    }

    //  Send message to a named group
    public void shout(String group, ZMsg request)
    {
        actor.shout(group, request);
    }

    // Returns lis tof current peers
    public List<String> peers()
    {
        return actor.peers();
    }

    // Returns lis tof current peers
    public List<String> peersByGroup(String group)
    {
        return actor.peersByGroup(group);
    }

    //  Return the endpoint of a connected peer. Returns empty string if
    //  the peer does not exist.
    public String peerEndpoint(String peer)
    {
        return actor.peerEndpoint(peer);
    }

    //  Return the value of a header of a connected peer.  Returns null if peer
    //  or key doesn't exits.
    public String peerHeader(String peer, String header)
    {
        return actor.peerHeader(peer, header);
    }

    //  Return zlist of currently joined groups.
    public List<String> ownGroups()
    {
        return actor.ownGroups();
    }

    //  Return zlist of groups known through connected peers.
    public List<String> peerGroups()
    {
        return actor.peerGroups();
    }

    public void destroy()
    {
        actor.terminate();
    }
}
