package db.postgresql.async.tasks;

import db.postgresql.async.messages.Notification;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class QueueingNotification extends NotificationTask {

    public QueueingNotification() {
        this(new ConcurrentLinkedQueue<>());
    }

    public QueueingNotification(final Queue<Notification> queue) {
        this(queue, new HashSet<>());
    }

    public QueueingNotification(final Queue<Notification> queue, final Set<String> subscribe) {
        this(queue, subscribe, 5L, TimeUnit.SECONDS);
    }
    
    public QueueingNotification(final Queue<Notification> queue, final Set<String> subscribe,
                                final long timeout, final TimeUnit units) {
        super(queue::add, subscribe, timeout, units);
    }
}
