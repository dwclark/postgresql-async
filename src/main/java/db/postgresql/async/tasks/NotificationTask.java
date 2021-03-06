package db.postgresql.async.tasks;

import db.postgresql.async.SessionInfo;
import db.postgresql.async.CompletableTask;
import db.postgresql.async.Task;
import db.postgresql.async.TaskState;
import db.postgresql.async.messages.BackEnd;
import db.postgresql.async.messages.FrontEndMessage;
import db.postgresql.async.messages.Notification;
import db.postgresql.async.messages.ReadyForQuery;
import db.postgresql.async.messages.Response;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import static java.util.stream.Collectors.joining;

//TODO: enforce write checks in front end message
public class NotificationTask extends BaseTask<Void> implements CompletableTask<Void> {

    private static class Action {
        final Consumer<Notification> consumer;
        final String channel;
        final CompletableFuture<Void> future;
        boolean submitted = false;

        public Action(final String channel, final Consumer<Notification> consumer) {
            this.channel = channel;
            this.consumer = consumer;
            this.future = new CompletableFuture<>();
        }
    }

    private final Consumer<Notification> DELETE = (n) -> {};
    private final ConcurrentMap<String,Consumer<Notification>> subscribed = new ConcurrentHashMap<>(50, 0.75f, 1);
    private final List<Action> actions = new CopyOnWriteArrayList<>();
    private volatile boolean shuttingDown = false;

    public NotificationTask() { }

    public void shutdown() {
        shuttingDown = true;
    }
    
    private boolean readProcessor(final Response resp) {
        switch(resp.getBackEnd()) {
        case CommandComplete:
            return true;
        case NotificationResponse:
            complete((Notification) resp);
            return true;
        case ReadyForQuery:
            complete((ReadyForQuery) resp);
            return false;
        default:
            throw new UnsupportedOperationException("Can't handle back end of type " + resp.getBackEnd());
        }
    }

    private void complete(final ReadyForQuery rfq) {
        if(actions.size() == 0) {
            return;
        }

        final Action action = actions.get(0);
        if(!action.submitted) {
            return;
        }

        if(action.consumer == DELETE) {
            subscribed.remove(action.channel);
        }

        actions.remove(0);
        action.future.complete(null);
    }

    private void complete(final Notification n) {
        final Consumer<Notification> consumer = subscribed.get(n.getChannel());
        if(consumer != null) {
            consumer.accept(n);
        }
    }

    private void maintenance(final FrontEndMessage fe) {
        if(shuttingDown) {
            nextState = TaskState.terminate();
            return;
        }
        
        if(actions.size() > 0) {
            final Action current = actions.get(0);
            if(current.consumer == DELETE) {
                fe.query(String.format("unlisten %s", current.channel));
            }
            else {
                fe.query(String.format("listen %s", current.channel));
                subscribed.put(current.channel, current.consumer);
            }

            actions.get(0).submitted = true;
            nextState = TaskState.write();
        }
    }

    private void computeNextState(final int needs, final FrontEndMessage fe) {
        if(needs > 0) {
            nextState = TaskState.needs(needs);
        }
        else if(actions.size() > 0) {
            maintenance(fe);
        }
        else {
            nextState = TaskState.finished();
        }
    }

    @Override
    public void onStart(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        if(actions.size() > 0) {
            maintenance(fe);
        }
        else {
            //force at least one trip to server and back
            fe.sync();
            nextState = TaskState.write();
        }
    }

    @Override
    public void onRead(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        computeNextState(pump(readBuffer, this::readProcessor), fe);
    }
    
    public CompletableFuture<Void> add(final String channel, final Consumer<Notification> consumer) {
        final Action action = new Action(channel, consumer);
        actions.add(action);
        return action.future;
    }

    public CompletableFuture<Void> remove(final String channel) {
        final Action action = new Action(channel, DELETE);
        actions.add(action);
        return action.future;
    }

    public Void getResult() {
        return null;
    }

    public CompletableFuture<Void> getFuture() {
        return null;
    }
}
