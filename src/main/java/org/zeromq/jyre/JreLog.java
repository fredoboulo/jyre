package org.zeromq.jyre;

import org.zeromq.zproto.annotation.protocol.Field;
import org.zeromq.zproto.annotation.protocol.Message;
import org.zeromq.zproto.annotation.protocol.Protocol;

@Protocol(signature = 0xAAA0 | 2)
class JreLog
{
    @Message(id = 1)
    static class Log extends JreLog
    {
        @Field
        byte    level;
        @Field
        byte    event;
        @Field
        short    node;
        @Field
        short    peer;
        @Field
        long   time;
        @Field
        String data;
    }
}
