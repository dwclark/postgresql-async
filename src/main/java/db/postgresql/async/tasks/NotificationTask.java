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
public class NotificationTask extends BaseTask<Void> {

    protected final Set<String> subscribed = new CopyOnWriteArraySet<>();
    protected final Set<String> unsubscribe = new CopyOnWriteArraySet<>();
    protected final Set<String> subscribe = new CopyOnWriteArraySet<>();
    private final Consumer<Notification> onNotification;

    public NotificationTask(final Consumer<Notification> onNotification) {
        this(onNotification, 5L, TimeUnit.SECONDS);
    }
    
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
        return maintenance(fe);
    }

    private boolean readProcessor(final Response resp) {
        switch(resp.getBackEnd()) {
        case CommandComplete:
            return true;
        case NotificationResponse:
            onNotification.accept((Notification) resp);
            return true;
        case ReadyForQuery:
            return false;
        default:
            throw new UnsupportedOperationException("Can't handle back end of type " + resp.getBackEnd());
        }
    }

    private TaskState maintenance(final FrontEndMessage fe) {
        final String toDo = getMaintenancePayload();
        if(toDo.length() > 0) {
            fe.query(toDo);
            return TaskState.write();
        }
        else {
            return TaskState.read();
        } 
    }
        
    public TaskState onRead(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        return pump(readBuffer, this::readProcessor, () -> maintenance(fe));
    }
    
    @Override
    public TaskState onTimeout(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        return maintenance(fe);
    }

    public void addSubscriptions(final Set<String> channels) {
        subscribe.addAll(channels);
    }

    public void removeSubscriptions(final Set<String> channels) {
        unsubscribe.addAll(channels);
    }

    public final String getMaintenancePayload() {
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
