package com.fabahaba.jedipus.cmds;

import com.fabahaba.jedipus.cluster.CRC16;
import com.fabahaba.jedipus.primitive.RedisOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CmdByteArray<R> {

  private static final byte DOLLAR_BYTE = '$';
  private static final byte ASTERISK_BYTE = '*';
  private static final byte[] CRLF = new byte[]{'\r', '\n'};
  private final Cmd<R> cmd;
  private final byte[] cmdArgs;
  private final int slot;

  private CmdByteArray(final Cmd<R> cmd, final byte[] cmdArgs, final int slot) {
    this.cmd = cmd;
    this.cmdArgs = cmdArgs;
    this.slot = slot;
  }

  public static <R> Builder<R> startBuilding(final Cmd<R> cmd) {
    return new ArrayListBuilder<>(cmd);
  }

  public static <R> Builder<R> startBuilding(final Cmd<R> cmd, final int numCmdAndArgs) {
    return new ArrayBuilder<>(cmd, numCmdAndArgs);
  }

  public Cmd<R> getCmd() {
    return cmd;
  }

  public byte[] getCmdArgs() {
    return cmdArgs;
  }

  public int getSlot() {
    return slot;
  }

  public abstract static class Builder<R> {

    private final Cmd<R> cmd;
    protected int numArgs;
    protected int numArgBytes;

    protected int offset;

    protected int slot = Integer.MIN_VALUE;

    private Builder(final Cmd<R> cmd) {

      this.cmd = cmd;
    }

    public CmdByteArray<R> create() {

      return create(cmd);
    }

    public abstract <O> CmdByteArray<O> create(final Cmd<O> overrideReturnTypeCmd);

    public abstract Builder<R> addArg(final byte[] arg);

    public abstract Builder<R> reset();

    protected byte[] createArray() {
      final byte[] asteriskLengthCRLF = RedisOutputStream.createIntCRLF(ASTERISK_BYTE, numArgs);
      offset = asteriskLengthCRLF.length;
      final byte[] cmdArgsBytes = new byte[numArgBytes + offset];

      System.arraycopy(asteriskLengthCRLF, 0, cmdArgsBytes, 0, offset);

      return cmdArgsBytes;
    }

    public Builder<R> addSlotKey(final String key) {
      return addSlotKey(RESP.toBytes(key));
    }

    public Builder<R> addSlotKey(final byte[] key) {
      this.slot = CRC16.getSlot(key);
      addArg(key);
      return this;
    }

    public Builder<R> setSlot(final int slot) {
      this.slot = slot;
      return this;
    }

    public Builder<R> addSubCmd(final Cmd<?> cmd) {
      return addArg(cmd.getCmdBytes());
    }

    public Builder<R> addSubCmd(final Cmd<?> cmd, final String arg) {
      addArg(cmd.getCmdBytes());
      return addArg(arg);
    }

    public Builder<R> addSubCmd(final Cmd<?> cmd, final String... args) {
      addArg(cmd.getCmdBytes());
      return addArgs(args);
    }

    public Builder<R> addSubCmd(final Cmd<?> cmd, final byte[] arg) {
      addArg(cmd.getCmdBytes());
      return addArg(arg);
    }

    public Builder<R> addSubCmd(final Cmd<?> cmd, final byte[]... args) {
      addArg(cmd.getCmdBytes());
      return addArgs(args);
    }

    public Builder<R> addArg(final boolean arg) {
      return addArg(RESP.toBytes(arg));
    }

    public Builder<R> addArgs(final boolean... args) {
      for (final boolean arg : args) {
        addArg(RESP.toBytes(arg));
      }
      return this;
    }

    public Builder<R> addArg(final long arg) {
      return addArg(RESP.toBytes(arg));
    }

    public Builder<R> addArgs(final long... args) {
      for (final long arg : args) {
        addArg(RESP.toBytes(arg));
      }
      return this;
    }

    public Builder<R> addArg(final int arg) {
      return addArg(RESP.toBytes(arg));
    }

    public Builder<R> addArgs(final int... args) {
      for (final int arg : args) {
        addArg(RESP.toBytes(arg));
      }
      return this;
    }

    public Builder<R> addArg(final double arg) {
      return addArg(RESP.toBytes(arg));
    }

    public Builder<R> addArgs(final double... args) {
      for (final double arg : args) {
        addArg(RESP.toBytes(arg));
      }
      return this;
    }

    public Builder<R> addArg(final String arg) {
      return addArg(RESP.toBytes(arg));
    }

    public Builder<R> addArgs(final String... args) {
      for (final String arg : args) {
        addArg(RESP.toBytes(arg));
      }
      return this;
    }

    public Builder<R> addArgs(final byte[]... args) {
      for (final byte[] arg : args) {
        addArg(arg);
      }
      return this;
    }
  }

  private static final class ArrayListBuilder<R> extends Builder<R> {

    private final List<byte[]> cmdArgs;

    private ArrayListBuilder(final Cmd<R> cmd) {
      this(cmd, 4);
    }

    private ArrayListBuilder(final Cmd<R> cmd, final int expectedCmdArgs) {
      super(cmd);
      this.cmdArgs = new ArrayList<>(expectedCmdArgs * 3);
      addArg(cmd.getCmdBytes());
    }

    @Override
    public <O> CmdByteArray<O> create(final Cmd<O> overrideReturnTypeCmd) {
      final byte[] cmdArgsBytes = createArray();

      for (final byte[] cmdArg : cmdArgs) {
        System.arraycopy(cmdArg, 0, cmdArgsBytes, offset, cmdArg.length);
        offset += cmdArg.length;
      }

      return new CmdByteArray<>(overrideReturnTypeCmd, cmdArgsBytes, slot);
    }

    @Override
    public Builder<R> reset() {
      cmdArgs.clear();
      numArgBytes = 0;
      offset = 0;
      numArgBytes = 0;
      return this;
    }

    @Override
    public Builder<R> addArg(final byte[] argBytes) {
      final byte[] dollarLengthCRLF = RedisOutputStream.createIntCRLF(DOLLAR_BYTE, argBytes.length);

      cmdArgs.add(dollarLengthCRLF);
      numArgBytes += dollarLengthCRLF.length;

      cmdArgs.add(argBytes);
      numArgBytes += argBytes.length;

      cmdArgs.add(CRLF);
      numArgBytes += 2;

      numArgs++;

      return this;
    }
  }

  private static final class ArrayBuilder<R> extends Builder<R> {

    private final byte[][] cmdArgs;
    private int index;

    private ArrayBuilder(final Cmd<R> cmd, final int numCmdAndArgs) {
      super(cmd);
      this.numArgs = numCmdAndArgs;
      this.cmdArgs = new byte[numArgs * 3][];
      this.index = 0;
      addArg(cmd.getCmdBytes());
    }

    @Override
    public <O> CmdByteArray<O> create(final Cmd<O> overrideReturnTypeCmd) {
      final byte[] cmdArgsBytes = createArray();

      for (final byte[] cmdArg : cmdArgs) {
        System.arraycopy(cmdArg, 0, cmdArgsBytes, offset, cmdArg.length);
        offset += cmdArg.length;
      }

      return new CmdByteArray<>(overrideReturnTypeCmd, cmdArgsBytes, slot);
    }

    @Override
    public Builder<R> reset() {
      Arrays.fill(cmdArgs, null);
      index = 0;
      offset = 0;
      numArgBytes = 0;
      return this;
    }

    @Override
    public Builder<R> addArg(final byte[] argBytes) {
      final byte[] dollarLengthCRLF = RedisOutputStream.createIntCRLF(DOLLAR_BYTE, argBytes.length);

      cmdArgs[index++] = dollarLengthCRLF;
      numArgBytes += dollarLengthCRLF.length;

      cmdArgs[index++] = argBytes;
      numArgBytes += argBytes.length;

      cmdArgs[index++] = CRLF;
      numArgBytes += 2;

      return this;
    }
  }
}
