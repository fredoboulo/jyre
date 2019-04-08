package org.zeromq.jyre;
/*  =========================================================================
    TestZreUDP - UDP test management class

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

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.zeromq.ZMsg;
import org.zyre.ZreInterface;

public class TestJyreInteroperability
{
    public static final int PING_INTERVAL = 1000;  //  Once per second
    public static final int PEER_EVASIVE  = 5000;  //  Five seconds' silence is evasive
    public static final int PEER_EXPIRED  = 10000; //  Ten seconds' silence is expired

    private static class ZrePing extends Thread
    {
        @Override
        public void run()
        {
            ZreInterface inf = new ZreInterface();

            while (true) {
                ZMsg incoming = inf.recv();

                if (incoming == null) {
                    break;
                }

                //  If new peer, say hello to it and wait for it to answer us
                String event = incoming.popString();
                if (event.equals("ENTER")) {
                    String identity = incoming.popString();
                    System.out.printf("I: [%s] peer entered\n", identity);
                }
                else if (event.equals("WHISPER")) {
                    String peer = incoming.popString();
                    String msg = incoming.popString();

                    if (msg.equals("HELLO")) {
                        ZMsg outgoing = new ZMsg();
                        outgoing.add(peer);
                        outgoing.add("WORLD");
                        inf.whisper(outgoing);
                    }

                    if (msg.equals("QUIT")) {
                        break;
                    }
                }
                else if (event.equals("SHOUT")) {
                    String identity = incoming.popString();
                    incoming.popString();
                    String msg = incoming.popString();

                    if (msg.equals("HELLO")) {
                        ZMsg outgoing = new ZMsg();
                        outgoing.add(identity);
                        outgoing.add("WORLD");
                        inf.whisper(outgoing);
                    }

                    if (msg.equals("QUIT")) {
                        break;
                    }
                }
                else if (event.equals("JOIN")) {
                    incoming.popString();
                    String group = incoming.popString();

                    inf.join(group);
                }
                else if (event.equals("LEAVE")) {
                    incoming.popString();
                    String group = incoming.popString();

                    inf.leave(group);
                }
            }
            inf.destroy();
        }
    }

    @Test
    public void testInterfaceWhisper() throws Exception
    {
        ZrePing ping = new ZrePing();
        ping.start();

        Jyre inf = new Jyre(null, "Whisper", PING_INTERVAL, PEER_EVASIVE, PEER_EXPIRED);

        ZMsg incoming = inf.agent.recv();

        String event = incoming.popString();
        assertEquals("ENTER", event);
        String peer = incoming.popString();

        ZMsg outgoing = new ZMsg();
        outgoing.add(peer);
        outgoing.add("HELLO");
        inf.whisper(outgoing);

        incoming = inf.agent.recv();
        event = incoming.popString();
        assertEquals("WHISPER", event);
        assertEquals(peer, incoming.popString());
        assertEquals("WORLD", incoming.popString());

        outgoing = new ZMsg();
        outgoing.add(peer);
        outgoing.add("QUIT");
        inf.whisper(outgoing);

        ping.join();
        inf.agent.close();
        inf.agent.exit().awaitSilent();
    }

    @Test
    public void testInterfaceGroup() throws Exception
    {
        String group = "TEST";

        ZrePing ping = new ZrePing();
        ping.start();

        Jyre inf = new Jyre(null, "Group", PING_INTERVAL, PEER_EVASIVE, PEER_EXPIRED);
        inf.join(group);

        ZMsg incoming = inf.agent.recv();

        String event = incoming.popString();
        assertEquals("ENTER", event);
        String peer = incoming.popString();

        incoming = inf.agent.recv();
        event = incoming.popString();
        assertEquals("JOIN", event);

        ZMsg outgoing = new ZMsg();
        outgoing.add(group);
        outgoing.add("HELLO");
        inf.shout(outgoing);

        incoming = inf.agent.recv();
        event = incoming.popString();
        assertEquals("WHISPER", event);
        assertEquals(peer, incoming.popString());
        assertEquals("WORLD", incoming.popString());

        inf.leave(group);

        incoming = inf.agent.recv();
        event = incoming.popString();
        assertEquals("LEAVE", event);

        outgoing = new ZMsg();
        outgoing.add(peer);
        outgoing.add("QUIT");
        inf.whisper(outgoing);

        ping.join();
        inf.agent.close();
        inf.agent.exit().awaitSilent();
    }

    @Test
    public void testInterfaceShout() throws Exception
    {
        String group = "TEST";

        ZrePing ping = new ZrePing();
        ping.start();

        ZrePing ping2 = new ZrePing();
        ping2.start();

        Jyre inf = new Jyre(null, "Shout", PING_INTERVAL, PEER_EVASIVE, PEER_EXPIRED);

        assertEquals("ENTER", inf.agent.recv().popString());
        assertEquals("ENTER", inf.agent.recv().popString());

        inf.join(group);

        assertEquals("JOIN", inf.agent.recv().popString());
        assertEquals("JOIN", inf.agent.recv().popString());

        ZMsg outgoing = new ZMsg();
        outgoing.add(group);
        outgoing.add("HELLO");
        inf.shout(outgoing);

        assertEquals("WHISPER", inf.agent.recv().popString());
        assertEquals("WHISPER", inf.agent.recv().popString());

        outgoing = new ZMsg();
        outgoing.add(group);
        outgoing.add("QUIT");
        inf.shout(outgoing);

        ping.join();
        ping2.join();
        inf.agent.close();
        inf.agent.exit().awaitSilent();
    }

    @Test
    public void testExit() throws Exception
    {
        ZrePing ping = new ZrePing();
        ping.start();

        Jyre inf = new Jyre(null, "Exit", 1000, 400, 800);

        ZMsg incoming = inf.agent.recv();

        String event = incoming.popString();
        assertEquals("ENTER", event);
        String peer = incoming.popString();

        ZMsg outgoing = new ZMsg();
        outgoing.add(peer);
        outgoing.add("QUIT");
        inf.whisper(outgoing);

        outgoing = new ZMsg();
        outgoing.add(peer);
        outgoing.add("QUIT");
        inf.whisper(outgoing);

        ping.join();

        // will take EXPIRED_PERIOD milliseconds
        incoming = inf.agent.recv();

        event = incoming.popString();
        assertEquals("EXIT", event);
        assertEquals(peer, incoming.popString());

        inf.agent.close();
        inf.agent.exit().awaitSilent();
    }
}
