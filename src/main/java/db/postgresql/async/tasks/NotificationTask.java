package db.postgresql.async.tasks;

import db.postgresql.async.SessionInfo;
import db.postgresql.async.CompletableTask;
import db.postgresql.async.Task;
import db.postgresql.async.TaskState;
import db.postgresql.async.messages.BackEnd;
import db.postgresql.async.messages.FrontEndMessage;
import db.postgresql.async.messages.Notification;
import db.postgresql.async.messages.Response;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import static java.util.stream.Collectors.joining;

//TODO: enforce write checks in front end message
public class NotificationTask extends BaseTask<Void> implements CompletableTask<Void> {

    private final Consumer<Notification> DELETE = (n) -> {};
    private final ConcurrentMap<String,Consumer<Notification>> subscribed = new ConcurrentHashMap<>(50, 0.75f, 1);
    private final ConcurrentMap<String,Consumer<Notification>> waiting = new ConcurrentHashMap<>(5, 0.9f, 1);
    private volatile boolean shuttingDown = false;

    public NotificationTask(final SessionInfo sessionInfo) {
        super(sessionInfo.getListeningTimeout(), sessionInfo.getListeningUnits());
        waiting.putAll(sessionInfo.getListeners());
    }

    public void shutdown() {
        shuttingDown = true;
    }
    
    public void onStart(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        maintenance(fe);
    }

    private boolean readProcessor(final Response resp) {
        switch(resp.getBackEnd()) {
        case CommandComplete:
            return true;
        case NotificationResponse:
            final Notification n = (Notification) resp;
            final Consumer<Notification> consumer = subscribed.get(n.getChannel());
            if(consumer != null) {
                consumer.accept(n);
            }

            return true;
        case ReadyForQuery:
            return false;
        default:
            throw new UnsupportedOperationException("Can't handle back end of type " + resp.getBackEnd());
        }
    }

    private void maintenance(final FrontEndMessage fe) {
        if(shuttingDown) {
            nextState = TaskState.terminate();
            return;
        }
        
        if(waiting.size() > 0) {
            fe.query(getMaintenancePayload());
            alterListeners();
            nextState = TaskState.write();
        }
        else {
            nextState = TaskState.read();
        } 
    }

    private void computeNextState(final int needs, final FrontEndMessage fe) {
        if(needs > 0) {
            nextState = TaskState.needs(needs);
        }
        else {
            maintenance(fe);
        }
    }

    @Override
    public void onRead(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        computeNextState(pump(readBuffer, this::readProcessor), fe);
    }
    
    @Override
    public void onTimeout(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        maintenance(fe);
    }

    public void add(final String key, final Consumer<Notification> consumer) {
        waiting.put(key, consumer);
    }

    public void remove(final String key) {
        waiting.put(key, DELETE);
    }

    private String getMaintenancePayload() {
        final StringBuilder builder = new StringBuilder();
        for(Map.Entry<String,Consumer<Notification>> entry : waiting.entrySet()) {
            if(entry.getValue() == DELETE) {
                builder.append("listen ").append(entry.getKey()).append(";");
            }
            else {
                builder.append("unlisten ").append(entry.getKey()).append(";");
            }
        }

        return builder.toString();
    }

    private void alterListeners() {
        for(Map.Entry<String,Consumer<Notification>> entry : waiting.entrySet()) {
            if(entry.getValue() == DELETE) {
                subscribed.remove(entry.getKey());
            }
            else {
                subscribed.put(entry.getKey(), entry.getValue());
            }
        }

        waiting.clear();
    }

    public Void getResult() {
        return null;
    }

    public CompletableFuture<Void> getFuture() {
        return null;
    }
}
