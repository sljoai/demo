package com.song.cn.linkedtable;

/**
 * 有两个单向链表（链表长度分别为 m，n），这两个单向链表有可能在某个元素合并，如下图所示的这样，也可能不合并。
 * 现在给定两个链表的头指针，在不修改链表的情况下，如何快速地判断这两个链表是否合并？如果合并，找到合并的元素，也就是图中的 x 元素。
 * 请用（伪）代码描述算法，并给出时间复杂度和空间复杂度。
 */
public class TwoLinkedTableWithCommonNode {

    public static void main(String[] args) {
        Node firstHead = new Node();
        Node aNode = new Node("a");
        firstHead.setNext(aNode);

        // 链表1 独立节点
        Node bNode = new Node("b");
        aNode.setNext(bNode);

        Node secondHead = new Node();
        Node dNode = new Node("d");
        secondHead.setNext(dNode);

        // 链表2 独立节点
        Node eNode = new Node("e");
        dNode.setNext(eNode);

        Node fNode = new Node("f");
        eNode.setNext(fNode);

        // 两个链表公共节点
        Node xNode = new Node("x");
        bNode.setNext(xNode);
        fNode.setNext(xNode);

        Node yNode = new Node("y");
        xNode.setNext(yNode);

        Node zNode = new Node("z");
        yNode.setNext(zNode);

        zNode.setNext(null);

        Node commonNode = findCommonNode(firstHead,secondHead);
        System.out.println(commonNode==null?"不合并":"合并节点为"+commonNode);
    }

    /**
     * 暴力查找两个链表中公共的节点
     * 时间复杂度为 O(m*n)
     *
     * @param firstHead  第一个链表头节点
     * @param secondHead 第二个链表头节点
     * @return 第一个公共节点。如果没有，则返回null
     */
    private static Node findCommonNode(Node firstHead, Node secondHead) {
        Node ret = null;
        // 排除空链的情况
        if (firstHead == null || secondHead == null) {
            return ret;
        }
        Node tmp1 = firstHead;
        Node tmp2 = secondHead;

        while (tmp1 != null) {
            Node tmp  = tmp2.getNext();
            while (tmp != null) {
                if (tmp == tmp1) {
                    break;
                }
                tmp = tmp.getNext();
            }
            if (tmp != null) {
                ret = tmp;
                break;
            } else {
                tmp1 = tmp1.getNext();
                tmp2 = tmp2.getNext();
            }
        }
        return ret;
    }
}

/**
 * 链表节点
 */
class Node {
    private Node next;

    private String name;

    public Node() {
    }

    public Node(String name) {
        this.name = name;
    }

    public Node getNext() {
        return next;
    }

    public void setNext(Node next) {
        this.next = next;
    }

    @Override
    public String toString() {
        return "Node{" +
                "next=" + next +
                ", name='" + name + '\'' +
                '}';
    }
}
