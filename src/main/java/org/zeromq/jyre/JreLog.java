package org.zeromq.jyre;

import org.zeromq.zproto.annotation.protocol.Field;
import org.zeromq.zproto.annotation.protocol.Message;
import org.zeromq.zproto.annotation.protocol.Protocol;

@Protocol(signature = 0xAAA0 | 2)
public class JreLog
{
    @Message
    public static class Log extends JreLog
    {
        @Field(length = 1)
        public int    level;
        @Field(length = 1)
        public int    event;
        @Field(length = 2)
        public int    node;
        @Field(length = 2)
        public int    peer;
        @Field(length = 8)
        public long   time;
        @Field
        public String data;
    }
}
