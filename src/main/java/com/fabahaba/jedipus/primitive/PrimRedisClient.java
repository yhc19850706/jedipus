package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.client.NodeMapper;
import com.fabahaba.jedipus.client.RedisPipeline;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cmds.ClientCmds;
import com.fabahaba.jedipus.cmds.Cmd;
import java.net.Socket;

class PrimRedisClient extends BaseRedisClient {

  static final Cmd<String> ASKING = Cmd.createStringReply("ASKING");
  private PrimPipeline pipeline;

  PrimRedisClient(final Node node, final ReplyMode replyMode, final NodeMapper nodeMapper,
      final Socket socket, final int soTimeoutMillis, final int outputBufferSize,
      final int inputBufferSize) {

    super(new PrimRedisConn(node, replyMode, nodeMapper, socket, soTimeoutMillis,
        outputBufferSize, inputBufferSize));
  }

  @Override
  public void close() {
    try {
      if (pipeline != null) {
        pipeline.close();
      }
    } finally {
      super.close();
    }
  }

  @Override
  public void resetState() {
    if (pipeline != null) {
      pipeline.close();
    }
    conn.resetState();
  }

  @Override
  public RedisPipeline pipeline() {
    if (pipeline != null) {
      return pipeline;
    }
    return pipeline = new PrimPipeline(this);
  }

  @Override
  public void asking() {
    sendCmd(ASKING);
  }

  @Override
  public String setClientName(final String clientName) {
    return sendCmd(ClientCmds.CLIENT, ClientCmds.CLIENT_SETNAME, clientName);
  }

  @Override
  public String[] getClientList() {
    return sendCmd(ClientCmds.CLIENT, ClientCmds.CLIENT_LIST).split("\n");
  }

  @Override
  public String getClientName() {
    return sendCmd(ClientCmds.CLIENT, ClientCmds.CLIENT_GETNAME);
  }

  @Override
  public void flush() {
    conn.flushOS();
  }
}
