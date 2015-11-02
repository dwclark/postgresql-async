package db.postgresql.async.tasks;

import db.postgresql.async.Task;
import db.postgresql.async.TaskState;
import db.postgresql.async.messages.BackEnd;
import db.postgresql.async.messages.FrontEndMessage;
import db.postgresql.async.messages.Notification;
import db.postgresql.async.messages.Response;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import static java.util.stream.Collectors.joining;

//TODO: enforce write checks in front end message
public class NotificationTask extends Task<Void> {

    protected final Set<String> subscribed = new CopyOnWriteArraySet<>();
    protected final Set<String> unsubscribe = new CopyOnWriteArraySet<>();
    protected final Set<String> subscribe = new CopyOnWriteArraySet<>();
    private final Consumer<Notification> onNotification;
    
    public NotificationTask(final Consumer<Notification> onNotification, 
                            final long timeout, final TimeUnit units) {
        super(timeout, units);
        this.onNotification = onNotification;
    }

    public NotificationTask(final Consumer<Notification> onNotification, Set<String> subscribe,
                            final long timeout, final TimeUnit units) {
        super(timeout, units);
        this.subscribe.addAll(subscribe);
        this.onNotification = onNotification;
    }
    
    public TaskState onStart(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        final String msg = subscribeUnsubscribe();
        if(msg.length() > 0) {
            fe.query(msg);
            return TaskState.write();
        }
        else {
            return TaskState.read();
        }
    }

    private boolean readProcessor(final Response resp) {
        final BackEnd be = resp.getBackEnd();
        if(be == BackEnd.CommandComplete) {
            return true;
        }
        else if(be == BackEnd.NotificationResponse) {
            onNotification.accept((Notification) resp);
            return true;
        }
        else if(be == BackEnd.ReadyForQuery) {
            return false;
        }
        else {
            throw new UnsupportedOperationException("Can't handle back end of type " + be);
        } 
    }
        
    public TaskState onRead(final FrontEndMessage fe, ByteBuffer readBuffer) {
        return pump(readBuffer, this::readProcessor,
                    () -> {
                        final String toDo = subscribeUnsubscribe();
                        if(toDo.length() > 0) {
                            fe.query(toDo);
                            return TaskState.write();
                        }
                        else {
                            return TaskState.read();
                        } });
    }
    
    @Override
    public TaskState onTimeout() {
        return TaskState.read();
    }

    public void addSubscriptions(final Set<String> channels) {
        subscribe.addAll(channels);
    }

    public void removeSubscriptions(final Set<String> channels) {
        unsubscribe.addAll(channels);
    }

    public final String subscribeUnsubscribe() {
        String toDo = "";
        if(subscribe.size() > 0) {
            toDo += subscribe.stream().collect(joining(";", "listen ", ""));
            subscribed.addAll(subscribe);
        }

        if(unsubscribe.size() > 0) {
            toDo += unsubscribe.stream().collect(joining(";", "unlisten ", ""));
            subscribed.removeAll(unsubscribe);
        }

        return toDo;
    }
}
