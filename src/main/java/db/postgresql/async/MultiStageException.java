package db.postgresql.async;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MultiStageException extends RuntimeException {

    private final List<Task<?>> failedTasks;

    public List<Task<?>> getFailedTasks() {
        return failedTasks;
    }

    public List<Throwable> getErrors() {
        return failedTasks.stream().map((t) -> t.getError()).collect(Collectors.toList());
    }
    
    public MultiStageException(final List<Task<?>> failedTasks) {
        this.failedTasks = Collections.unmodifiableList(failedTasks);
    }

    @Override
    public String getMessage() {
        return failedTasks.stream()
            .map((t) -> t.getError().getMessage())
            .collect(Collectors.joining("\n"));
    }

    @Override
    public String toString() {
        return getMessage();
    }
}
