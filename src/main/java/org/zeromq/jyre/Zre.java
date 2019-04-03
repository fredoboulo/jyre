package org.zeromq.jyre;

import java.util.List;
import java.util.Map;

import org.zeromq.ZFrame;
import org.zeromq.zproto.annotation.protocol.Field;
import org.zeromq.zproto.annotation.protocol.Message;
import org.zeromq.zproto.annotation.protocol.Protocol;

@Protocol(signature = 0xAAA0 | 1)
public class Zre
{
    @Field(length = 2)
    int sequence;

    @Message(id = 1)
    public static class Hello extends Zre
    {
        @Field
        public String              ipaddress;
        @Field(length = 2)
        public int                 mailbox;
        @Field
        public List<String>        groups;
        @Field(length = 1)
        public int                 status;
        @Field
        public Map<String, String> headers;
    }

    @Message(id = 2)
    public static class Whisper extends Zre
    {
        @Field
        public ZFrame content;
    }

    @Message(id = 3)
    public static class Shout extends Zre
    {
        @Field
        public String group;
        @Field
        public ZFrame content;
    }

    @Message(id = 4)
    public static class Join extends Zre
    {
        @Field
        public String group;
        @Field(length = 1)
        public int    status;
    }

    @Message(id = 5)
    public static class Leave extends Zre
    {
        @Field
        public String group;
        @Field(length = 1)
        public int    status;
    }

    @Message(id = 6)
    public static class Ping extends Zre
    {
    }

    @Message(id = 7)
    public static class PingOk extends Zre
    {
    }
}
