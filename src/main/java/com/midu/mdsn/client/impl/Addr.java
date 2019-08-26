package com.midu.mdsn.client.impl;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author https://xujinbang.com/
 * @package com.midu.mdsn.client.impl
 * @date 19-7-10
 */
class Addr {

    private Node[] nodes;

    private String mode;

    Addr(Node[] nodeList, String model) {
        nodes = nodeList;
        mode = model;
        calcList(2);

        CalcThread calc = new CalcThread(this);
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setThreadFactory(calc).setDaemon(true).build();
        ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);
        scheduledExecutor.schedule(calc, 10, TimeUnit.SECONDS);
    }

    class CalcThread implements Runnable, ThreadFactory {
        Addr a;

        CalcThread(Addr addr) {
            a = addr;
        }

        public Thread newThread(Runnable r) {
            return new Thread(this);
        }

        public void run() {
            a.calcList(2);
        }
    }

    void calcList(int count) {
        count = count <= 0 ? 1 : count;
        for (Node n : nodes) {
            int time = executePing(n.IP, count);
            if (time < 0) {
                n.status = 0;
            }
            n.status = 1;
            n.icmpTime = time;
        }

        Arrays.sort(nodes, new Comparator<Node>() {
            public int compare(Node n1, Node n2) {
                if (n1.status == 0 || n2.status == 0) {
                    return 1;
                }
                return n1.icmpTime - n2.icmpTime;
            }
        });
    }

    int executePing(String ip, int count) {
        // System.out.println("execute ping...");
        String fullCommand;
        String keywordForBreak, successMessage, failMessage;
        String os = System.getProperty("os.name");
        // TODO Add proper support for finding out if ifconfig exists
        if (os.startsWith("Windows")) {
            fullCommand = String.format("ping %s -n %d", ip, count);
            keywordForBreak = "Reply";
            successMessage = "time";
            failMessage = "unreachable";
        } else {
            fullCommand = String.format("ping %s -c %d", ip, count);
            keywordForBreak = "packets";
            successMessage = "1 received";
            failMessage = "0 received";
        }
        long s = System.currentTimeMillis();
        try {
            String output = "";
            Process p = Runtime.getRuntime().exec(fullCommand);
            BufferedReader inputStream = new BufferedReader(new InputStreamReader(p.getInputStream()));

            while ((output = inputStream.readLine()) != null) {
                if (output.contains(keywordForBreak)) {
                    break;
                }
            }
            if (output.contains(failMessage)) {
                return -1;
            }
            if (output.contains(successMessage)) {
                long e = System.currentTimeMillis();
                long r = e - s;
                return (int) r;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    Node getOnline() {
        for (Node n : nodes)
            if (n.status == 1)
                return n;
        return null;
    }


}
