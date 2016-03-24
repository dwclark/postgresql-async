package db.postgresql.async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class IOCompletableFuture<T> implements CompletionStage<T> {

    private final CompletableFuture<T> myFuture;
    private final Executor myExecutor;

    public IOCompletableFuture(final CompletableFuture<T> f, final Executor e) {
        myFuture = f;
        myExecutor = e;
    }

    public static <T> IOCompletableFuture<T> supplyAsync(final Supplier<T> s, final Executor e) {
        return _wrap(CompletableFuture.supplyAsync(s, e), e);
    }

    public static <T> IOCompletableFuture<Void> runAsync(final Runnable r, final Executor e) {
        return _wrap(CompletableFuture.runAsync(r, e), e);
    }

    private static <T> CompletableFuture<T> _wrap(final CompletableFuture<T> f, final Executor e) {
        final IOCompletableFuture<T> myFuture = new IOCompletableFuture<>(f, e);
        f.whenComplete((v,t)-> {
                if(t == null) {
                    myFuture.complete(v);
                }
                else {
                    myFuture.completeExceptionally(t);
                }
            });
        
        return myFuture;
    }

    public boolean isDone() {
        return myFuture.isDone();
    }

    public T get() throws InterruptedException, ExecutionException {
        return myFuture.get();
    }

    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return myFuture.get(timeout, unit);
    }

    @Override
    public T join() {
        return myFuture.join();
    }

    @Override
    public T getNow(T valueIfAbsent) {
        return myFuture.getNow(valueIfAbsent);
    }

    @Override
    public boolean complete(T value) {
        return myFuture.complete(value);
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        return myFuture.completeExceptionally(ex);
    }

    @Override
    public <U> IOCompletableFuture<U> thenApply(Function<? super T, ? extends U> function) {
        return _wrap(myFuture.thenApply(function), myExecutor);
    }

    @Override
    public <U> IOCompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> function) {
        return _wrap(myFuture.thenApplyAsync(function, myExecutor), myExecutor);
    }

    @Override
    public <U> IOCompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> function, Executor executor) {
        throw new UnsupportedOperationException("IOCompletableFutures are bound to their IO Pool, please use alternate thenApplyAsync method");
    }

    @Override
    public IOCompletableFuture<Void> thenAccept(Consumer<? super T> consumer) {
        return null;
    }

    @Override
    public IOCompletableFuture<Void> thenAcceptAsync(Consumer<? super T> consumer) {
        return null;
    }

    @Override
    public IOCompletableFuture<Void> thenAcceptAsync(Consumer<? super T> consumer, Executor executor) {
        return null;
    }

    @Override
    public IOCompletableFuture<Void> thenRun(Runnable runnable) {
        return null;
    }

    @Override
    public IOCompletableFuture<Void> thenRunAsync(Runnable runnable) {
        return null;
    }

    @Override
    public IOCompletableFuture<Void> thenRunAsync(Runnable runnable, Executor executor) {
        return null;
    }

    @Override
    public <U, V> IOCompletableFuture<V> thenCombine(CompletionStage<? extends U> completionStage, BiFunction<? super T, ? super U, ? extends V> biFunction) {
        return null;
    }

    @Override
    public <U, V> IOCompletableFuture<V> thenCombineAsync(CompletionStage<? extends U> completionStage, BiFunction<? super T, ? super U, ? extends V> biFunction) {
        return null;
    }

    @Override
    public <U, V> IOCompletableFuture<V> thenCombineAsync(CompletionStage<? extends U> completionStage, BiFunction<? super T, ? super U, ? extends V> biFunction, Executor executor) {
        return null;
    }

    @Override
    public <U> IOCompletableFuture<Void> thenAcceptBoth(CompletionStage<? extends U> completionStage, BiConsumer<? super T, ? super U> biConsumer) {
        return null;
    }

    @Override
    public <U> IOCompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> completionStage, BiConsumer<? super T, ? super U> biConsumer) {
        return null;
    }

    @Override
    public <U> IOCompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> completionStage, BiConsumer<? super T, ? super U> biConsumer, Executor executor) {
        return null;
    }

    @Override
    public IOCompletableFuture<Void> runAfterBoth(CompletionStage<?> completionStage, Runnable runnable) {
        return null;
    }

    @Override
    public IOCompletableFuture<Void> runAfterBothAsync(CompletionStage<?> completionStage, Runnable runnable) {
        return null;
    }

    @Override
    public IOCompletableFuture<Void> runAfterBothAsync(CompletionStage<?> completionStage, Runnable runnable, Executor executor) {
        return null;
    }

    @Override
    public <U> IOCompletableFuture<U> applyToEither(CompletionStage<? extends T> completionStage, Function<? super T, U> function) {
        return null;
    }

    @Override
    public <U> IOCompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> completionStage, Function<? super T, U> function) {
        return null;
    }

    @Override
    public <U> IOCompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> completionStage, Function<? super T, U> function, Executor executor) {
        return null;
    }

    @Override
    public IOCompletableFuture<Void> acceptEither(CompletionStage<? extends T> completionStage, Consumer<? super T> consumer) {
        return null;
    }

    @Override
    public IOCompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> completionStage, Consumer<? super T> consumer) {
        return null;
    }

    @Override
    public IOCompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> completionStage, Consumer<? super T> consumer, Executor executor) {
        return null;
    }

    @Override
    public IOCompletableFuture<Void> runAfterEither(CompletionStage<?> completionStage, Runnable runnable) {
        return null;
    }

    @Override
    public IOCompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> completionStage, Runnable runnable) {
        return null;
    }

    @Override
    public IOCompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> completionStage, Runnable runnable, Executor executor) {
        return null;
    }

    @Override
    public <U> IOCompletableFuture<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> function) {
        return null;
    }

    @Override
    public <U> IOCompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> function) {
        return null;
    }

    @Override
    public <U> IOCompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> function, Executor executor) {
        return null;
    }

    @Override
    public IOCompletableFuture<T> exceptionally(Function<Throwable, ? extends T> function) {
        return null;
    }

    @Override
    public IOCompletableFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> biConsumer) {
        return null;
    }

    @Override
    public IOCompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> biConsumer) {
        return null;
    }

    @Override
    public IOCompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> biConsumer, Executor executor) {
        return null;
    }

    @Override
    public <U> IOCompletableFuture<U> handle(BiFunction<? super T, Throwable, ? extends U> biFunction) {
        return null;
    }

    @Override
    public <U> IOCompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> biFunction) {
        return null;
    }

    @Override
    public <U> IOCompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> biFunction, Executor executor) {
        return null;
    }

    @Override
    public CompletableFuture<T> toCompletableFuture() {
        return myFuture;
    }
}
