/*  =========================================================================
    ZrePeer - one of our peers in a ZyRE network

    -------------------------------------------------------------------------
    Copyright (c) 1991-2012 iMatix Corporation <www.imatix.com>
    Copyright other contributors as noted in the AUTHORS file.

    This file is part of ZyRE, the ZeroMQ Realtime Experience framework:
    http://zyre.org.

    This is free software; you can redistribute it and/or modify it under
    the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation; either version 3 of the License, or (at
    your option) any later version.

    This software is distributed in the hope that it will be useful, but
    WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
    Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with this program. If not, see
    <http://www.gnu.org/licenses/>.
    =========================================================================
*/
package org.zeromq.jyre;

import java.util.HashMap;
import java.util.Map;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;

import zmq.util.Clock;

public class ZrePeer
{
    //  Constants, to be configured/reviewed
    public static final int PEER_EVASIVE = 5000;   //  Five seconds' silence is evasive
    private static final int PEER_EXPIRED = 10000; //  Ten seconds' silence is expired

    private static final short USHORT_MAX = (short) 0xffff;

    private final ZContext      ctx;           //  CZMQ context
    private Socket              mailbox;       //  Socket through to peer
    private final String        identity;      //  Identity string
    private String              endpoint;      //  Endpoint connected to
    private long                evasiveAt;     //  Peer is being evasive
    private long                expiredAt;     //  Peer has expired by now
    private boolean             connected;     //  Peer will send messages
    private boolean             ready;         //  Peer has said Hello to us
    private byte                status;        //  Our status counter
    private short               sentSequence;  //  Outgoing message sequence
    private short               wantSequence;  //  Incoming message sequence
    private Map<String, String> headers;       //  Peer headers

    private ZrePeer(ZContext ctx, String identity)
    {
        this.ctx = ctx;
        this.identity = identity;

        ready = false;
        connected = false;
        sentSequence = 0;
        wantSequence = 0;
    }

    //  ---------------------------------------------------------------------
    //  Construct new peer object
    static ZrePeer newPeer(String identity, Map<String, ZrePeer> container, ZContext ctx)
    {
        ZrePeer peer = new ZrePeer(ctx, identity);
        container.put(identity, peer);

        return peer;
    }

    //  ---------------------------------------------------------------------
    //  Destroy peer object
    public void destroy()
    {
        disconnect();
    }

    //  ---------------------------------------------------------------------
    //  Connect peer mailbox
    //  Configures mailbox and connects to peer's router endpoint
    public void connect(String replyTo, String endpoint)
    {
        //  Create new outgoing socket (drop any messages in transit)
        mailbox = ctx.createSocket(SocketType.DEALER);

        //  Null if shutting down
        if (mailbox != null) {
            //  Set our caller 'From' identity so that receiving node knows
            //  who each message came from.
            mailbox.setIdentity(replyTo.getBytes());

            //  Set a high-water mark that allows for reasonable activity
            mailbox.setSndHWM(PEER_EXPIRED * 100);

            //  Send messages immediately or return EAGAIN
            mailbox.setSendTimeOut(0);

            //  Connect through to peer node
            mailbox.connect(String.format("tcp://%s", endpoint));
            this.endpoint = endpoint;
            connected = true;
            ready = false;
        }
    }

    //  ---------------------------------------------------------------------
    //  Disconnect peer mailbox
    //  No more messages will be sent to peer until connected again
    void disconnect()
    {
        ctx.destroySocket(mailbox);
        mailbox = null;
        endpoint = null;
        connected = false;
    }

    public boolean send(ZreMsg msg)
    {
        if (connected) {
            msg.payload.sequence = ++sentSequence;
            if (!msg.send(mailbox)) {
                disconnect();
                return false;
            }
        }
        else {
            msg.destroy();
        }

        return true;
    }

    //  ---------------------------------------------------------------------
    //  Return peer connection endpoint
    String endpoint()
    {
        if (connected) {
            return endpoint;
        }
        else {
            return "";
        }

    }

    //  ---------------------------------------------------------------------
    //  Register activity at peer
    void refresh(int peerEvasive, int peerExpired)
    {
        long now = Clock.nowMS();
        evasiveAt = now + peerEvasive;
        expiredAt = now + peerExpired;
    }

    //  ---------------------------------------------------------------------
    //  Return peer future expired time
    long expiredAt()
    {
        return expiredAt;
    }

    //  ---------------------------------------------------------------------
    //  Return peer future evasive time
    long evasiveAt()
    {
        return evasiveAt;
    }

    void setReady(boolean ready)
    {
        this.ready = ready;
    }

    boolean ready()
    {
        return ready;
    }

    public void setStatus(byte status)
    {
        this.status = status;
    }

    public byte status()
    {
        return status;
    }

    void incStatus()
    {
        ++status;
    }

    String identity()
    {
        return identity;
    }

    public String header(String key, String defaultValue)
    {
        if (headers.containsKey(key)) {
            return headers.get(key);
        }

        return defaultValue;
    }

    void setHeaders(Map<String, String> headers)
    {
        this.headers = new HashMap<>(headers);
    }

    boolean checkMessage(Zre msg)
    {
        int receivedSequence = msg.sequence;
        ++wantSequence;

        boolean valid = wantSequence == receivedSequence;
        if (!valid) {
            if (--wantSequence < 0) {
                wantSequence = USHORT_MAX;
            }
        }
        return valid;
    }

}
