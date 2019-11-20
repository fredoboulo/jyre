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
    @Field
    short sequence;

    @Message(id = 1)
    static class Hello extends Zre
    {
        @Field
        String              ipaddress;
        @Field
        short               mailbox;
        @Field
        List<String>        groups;
        @Field
        byte                status;
        @Field
        Map<String, String> headers;
    }

    @Message(id = 2)
    static class Whisper extends Zre
    {
        @Field
        ZFrame content;
    }

    @Message(id = 3)
    static class Shout extends Zre
    {
        @Field
        String group;
        @Field
        ZFrame content;
    }

    @Message(id = 4)
    static class Join extends Zre
    {
        @Field
        String group;
        @Field
        byte   status;
    }

    @Message(id = 5)
    static class Leave extends Zre
    {
        @Field
        String group;
        @Field
        byte   status;
    }

    @Message(id = 6)
    static class Ping extends Zre
    {
    }

    @Message(id = 7)
    static class PingOk extends Zre
    {
    }
}
