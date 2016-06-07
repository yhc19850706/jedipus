package com.fabahaba.jedipus.client;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.util.Optional;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.junit.Test;

import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.primitive.RedisClientFactory;

public class SSLClientTest {

  public static final Path JCEKS_TRUSTSTORE =
      Paths.get(Optional.ofNullable(System.getProperty("jedipus.redis.ssl.truststore.jceks"))
          .orElse("stunnel/integ.jks"));

  public static final int REDIS_SSL_PORT = Optional
      .ofNullable(System.getProperty("jedipus.redis.ssl.port")).map(Integer::parseInt).orElse(6443);

  public static final Node DEFAULT_SSL_NODE = Node.create("localhost", REDIS_SSL_PORT);

  @Test
  public void connectAndPing() {
    System.setProperty("javax.net.ssl.trustStore", JCEKS_TRUSTSTORE.toString());
    System.setProperty("javax.net.ssl.trustStoreType", "jceks");

    try (final RedisClient client = RedisClientFactory.startBuilding()
        .withAuth(BaseRedisClientTest.REDIS_PASS)
        .withSocketFactory(SSLSocketFactory.getDefault()::createSocket).create(DEFAULT_SSL_NODE)) {
      final String ssl = client.sendCmd(Cmds.PING, "SSL");
      assertEquals("SSL", ssl);
    }
  }

  @Test
  public void useNonDefaultSocketFactory() {
    final IOFactory<Socket> sslSocketFactory = createTrustStoreSslSocketFactory();

    try (final RedisClient client =
        RedisClientFactory.startBuilding().withAuth(BaseRedisClientTest.REDIS_PASS)
            .withSocketFactory(sslSocketFactory).create(DEFAULT_SSL_NODE)) {
      final String ssl = client.sendCmd(Cmds.PING, "SSL");
      assertEquals("SSL", ssl);
    }
  }

  private static IOFactory<Socket> createTrustStoreSslSocketFactory() {
    try {
      final KeyStore trustStore = KeyStore.getInstance("jceks");

      try (final InputStream inputStream = Files.newInputStream(JCEKS_TRUSTSTORE)) {
        trustStore.load(inputStream, null);
      } catch (IOException | NoSuchAlgorithmException | CertificateException e) {
        throw new RuntimeException(e);
      }

      final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("PKIX");
      trustManagerFactory.init(trustStore);
      final TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();

      final SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(null, trustManagers, new SecureRandom());

      return sslContext.getSocketFactory()::createSocket;
    } catch (KeyManagementException | KeyStoreException | NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }
}
