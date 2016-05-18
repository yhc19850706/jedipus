package com.fabahaba.jedipus.cluster;

import com.fabahaba.jedipus.client.HostPort;
import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.client.RedisPipeline;
import com.fabahaba.jedipus.cmds.Cmd;
import com.fabahaba.jedipus.cmds.PrimArrayCmd;
import com.fabahaba.jedipus.cmds.PrimCmd;
import com.fabahaba.jedipus.cmds.RESP;

public class MockRedisClient implements RedisClient {

  @Override
  public int getSoTimeout() {

    return 0;
  }

  @Override
  public HostPort getHostPort() {

    return null;
  }

  @Override
  public boolean isBroken() {

    return false;
  }

  @Override
  public void close() {

  }

  @Override
  public Node getNode() {

    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd) {

    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd) {

    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[]... args) {

    return null;
  }

  @Override
  public <T> T sendBlockingCmd(final Cmd<T> cmd) {

    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final byte[]... args) {

    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final String... args) {

    return null;
  }

  @Override
  public <T> T sendBlockingCmd(final Cmd<T> cmd, final byte[]... args) {

    return null;
  }

  @Override
  public <T> T sendBlockingCmd(final Cmd<T> cmd, final String... args) {

    return null;
  }

  @Override
  public RedisPipeline pipeline() {

    return null;
  }

  @Override
  public void resetState() {

  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[] args) {

    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final byte[] arg) {

    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final String arg) {

    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final String... args) {

    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final String arg) {

    return null;
  }

  @Override
  public long sendCmd(final PrimCmd cmd) {

    return 0;
  }

  @Override
  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd) {

    return 0;
  }

  @Override
  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[] arg) {

    return 0;
  }

  @Override
  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[]... args) {

    return 0;
  }

  @Override
  public long sendCmd(final PrimCmd cmd, final byte[] arg) {

    return 0;
  }

  @Override
  public long sendCmd(final PrimCmd cmd, final byte[]... args) {

    return 0;
  }

  @Override
  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final String arg) {

    return 0;
  }

  @Override
  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final String... args) {

    return 0;
  }

  @Override
  public long sendCmd(final PrimCmd cmd, final String arg) {

    return 0;
  }

  @Override
  public long sendCmd(final PrimCmd cmd, final String... args) {

    return 0;
  }

  @Override
  public long sendBlockingCmd(final PrimCmd cmd) {

    return 0;
  }

  @Override
  public long sendBlockingCmd(final PrimCmd cmd, final byte[]... args) {

    return 0;
  }

  @Override
  public long sendBlockingCmd(final PrimCmd cmd, final String... args) {

    return 0;
  }

  @Override
  public String replyOn() {

    return RESP.OK;
  }

  @Override
  public RedisClient replyOff() {

    return null;
  }

  @Override
  public RedisClient skip() {

    return null;
  }

  @Override
  public void asking() {}


  @Override
  public String setClientName(final String clientName) {

    return null;
  }


  @Override
  public String getClientName() {

    return null;
  }


  @Override
  public String[] getClientList() {

    return null;
  }


  @Override
  public long[] sendCmd(final PrimArrayCmd cmd) {
    // TODO Auto-generated method stub
    return null;
  }


  @Override
  public long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd) {
    // TODO Auto-generated method stub
    return null;
  }


  @Override
  public long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final byte[] arg) {
    // TODO Auto-generated method stub
    return null;
  }


  @Override
  public long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final byte[]... args) {
    // TODO Auto-generated method stub
    return null;
  }


  @Override
  public long[] sendCmd(final PrimArrayCmd cmd, final byte[] arg) {
    // TODO Auto-generated method stub
    return null;
  }


  @Override
  public long[] sendCmd(final PrimArrayCmd cmd, final byte[]... args) {
    // TODO Auto-generated method stub
    return null;
  }


  @Override
  public long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final String... args) {
    // TODO Auto-generated method stub
    return null;
  }


  @Override
  public long[] sendCmd(final PrimArrayCmd cmd, final String... args) {
    // TODO Auto-generated method stub
    return null;
  }


  @Override
  public long[] sendBlockingCmd(final PrimArrayCmd cmd) {
    // TODO Auto-generated method stub
    return null;
  }


  @Override
  public long[] sendBlockingCmd(final PrimArrayCmd cmd, final byte[]... args) {
    // TODO Auto-generated method stub
    return null;
  }


  @Override
  public long[] sendBlockingCmd(final PrimArrayCmd cmd, final String... args) {
    // TODO Auto-generated method stub
    return null;
  }
}
