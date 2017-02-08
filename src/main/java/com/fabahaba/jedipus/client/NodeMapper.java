package com.fabahaba.jedipus.client;

import com.fabahaba.jedipus.cluster.Node;
import java.io.Serializable;
import java.util.function.Function;

public interface NodeMapper extends Function<Node, Node>, Serializable {

}
