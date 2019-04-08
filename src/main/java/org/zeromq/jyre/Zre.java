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

    @Message
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

    @Message
    public static class Whisper extends Zre
    {
        @Field
        public ZFrame content;
    }

    @Message
    public static class Shout extends Zre
    {
        @Field
        public String group;
        @Field
        public ZFrame content;
    }

    @Message
    public static class Join extends Zre
    {
        @Field
        public String group;
        @Field(length = 1)
        public int    status;
    }

    @Message
    public static class Leave extends Zre
    {
        @Field
        public String group;
        @Field(length = 1)
        public int    status;
    }

    @Message
    public static class Ping extends Zre
    {
    }

    @Message
    public static class PingOk extends Zre
    {
    }
}
