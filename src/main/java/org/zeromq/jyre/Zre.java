package org.zeromq.jyre;

import java.util.List;
import java.util.Map;

import org.zeromq.ZFrame;
import org.zeromq.zproto.annotation.protocol.Field;
import org.zeromq.zproto.annotation.protocol.Message;
import org.zeromq.zproto.annotation.protocol.Protocol;

@Protocol(signature = 0xAAA0 | 1)
class Zre
{
    @Field byte  version; // version number
    @Field short sequence;// Cyclic sequence number

    @Message(id = 1) //     Greet a peer so it can connect back to us
    static class Hello extends Zre
    {
        @Field String              endpoint;// Sender connect endpoint
        @Field List<String>        groups;  // List of groups sender is in<
        @Field byte                status;  // Sender groups status value
        @Field String              name;    // Sender public name
        @Field Map<String, String> headers; // Sender header properties
    }

    @Message(id = 2) //    Send a multi-part message to a peer
    static class Whisper extends Zre
    {
        @Field ZFrame content; // Wrapped message content TODO msg or frame ?
    }

    @Message(id = 3) //    Send a multi-part message to a group
    static class Shout extends Zre
    {
        @Field String group;    // Group to send to
        @Field ZFrame content;  // Wrapped message content TODO msg or frame ?
    }

    @Message(id = 4) //    Join a group
    static class Join extends Zre
    {
        @Field String group;    // Name of group
        @Field byte   status;   // Sender groups status value
    }

    @Message(id = 5) //    Leave a group
    static class Leave extends Zre
    {
        @Field String group;    // Name of group
        @Field byte   status;   // Sender groups status value
    }

    @Message(id = 6) //     Ping a peer that has gone silent
    static class Ping extends Zre
    {
    }

    @Message(id = 7) //    Reply to a peer's ping
    static class PingOk extends Zre
    {
    }

    @Message(id = 8)
    static class Elect extends Zre
    {
        @Field String group;        // Name of group
        @Field String challengerId; // ID of the challenger
    }

    @Message(id = 9)
    static class Leader extends Zre
    {
        @Field String group;    // Name of group
        @Field String leaderId; // ID of the elected leader
    }
}
