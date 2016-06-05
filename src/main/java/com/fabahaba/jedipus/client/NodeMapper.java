package com.fabahaba.jedipus.client;

import java.io.Serializable;
import java.util.function.Function;

import com.fabahaba.jedipus.cluster.Node;

public interface NodeMapper extends Function<Node, Node>, Serializable {

}
