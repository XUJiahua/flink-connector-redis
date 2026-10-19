/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.redis.table;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Minimal RESP server on an ephemeral loopback port. It only exists so that {@link
 * RedisSinkWriter}'s constructor can open its Lettuce connection without a real Redis; tests then
 * replace the commands container with a fake. HELLO is rejected (Lettuce falls back to RESP2),
 * PING answers PONG, everything else answers OK.
 */
final class FakeRespServer implements AutoCloseable {

    private final CountDownLatch hsetReceived = new CountDownLatch(1);
    private final ServerSocket serverSocket;
    private final List<Socket> sockets = new ArrayList<>();
    private final Thread acceptor;

    FakeRespServer() throws IOException {
        serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        acceptor = new Thread(this::acceptLoop, "fake-resp-acceptor");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    int port() {
        return serverSocket.getLocalPort();
    }

    private void acceptLoop() {
        while (!serverSocket.isClosed()) {
            try {
                Socket s = serverSocket.accept();
                synchronized (sockets) {
                    sockets.add(s);
                }
                Thread t = new Thread(() -> serve(s), "fake-resp-conn");
                t.setDaemon(true);
                t.start();
            } catch (IOException e) {
                return;
            }
        }
    }

    boolean awaitHset(long timeoutSeconds) throws InterruptedException {
        return hsetReceived.await(timeoutSeconds, TimeUnit.SECONDS);
    }

    private void serve(Socket s) {
        try (InputStream in = new BufferedInputStream(s.getInputStream());
                OutputStream out = s.getOutputStream()) {
            while (true) {
                List<String> cmd = readCommand(in);
                if (cmd == null) {
                    return;
                }
                String name = cmd.isEmpty() ? "" : cmd.get(0).toUpperCase(Locale.ROOT);
                String reply;
                if ("HELLO".equals(name)) {
                    reply = "-ERR unknown command 'HELLO'\r\n";
                } else if ("PING".equals(name)) {
                    reply = "+PONG\r\n";
                } else if ("HSET".equals(name)) {
                    hsetReceived.countDown();
                    reply = ":1\r\n";
                } else {
                    reply = "+OK\r\n";
                }
                out.write(reply.getBytes(StandardCharsets.UTF_8));
                out.flush();
            }
        } catch (IOException ignored) {
            // connection closed
        }
    }

    private static List<String> readCommand(InputStream in) throws IOException {
        String header = readLine(in);
        if (header == null) {
            return null;
        }
        List<String> parts = new ArrayList<>();
        if (!header.startsWith("*")) {
            // inline command
            for (String p : header.trim().split("\\s+")) {
                parts.add(p);
            }
            return parts;
        }
        int n = Integer.parseInt(header.substring(1));
        for (int i = 0; i < n; i++) {
            String lenLine = readLine(in);
            if (lenLine == null) {
                return null;
            }
            int len = Integer.parseInt(lenLine.substring(1));
            byte[] buf = new byte[len];
            int off = 0;
            while (off < len) {
                int r = in.read(buf, off, len - off);
                if (r < 0) {
                    return null;
                }
                off += r;
            }
            readLine(in); // trailing CRLF
            parts.add(new String(buf, StandardCharsets.UTF_8));
        }
        return parts;
    }

    private static String readLine(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) >= 0) {
            if (c == '\r') {
                int next = in.read();
                if (next == '\n' || next < 0) {
                    return sb.toString();
                }
                sb.append((char) c).append((char) next);
            } else {
                sb.append((char) c);
            }
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    @Override
    public void close() throws IOException {
        serverSocket.close();
        synchronized (sockets) {
            for (Socket s : sockets) {
                try {
                    s.close();
                } catch (IOException ignored) {
                    // best effort
                }
            }
        }
    }
}
