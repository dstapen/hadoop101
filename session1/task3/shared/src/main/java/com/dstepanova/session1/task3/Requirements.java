package com.dstepanova.session1.task3;

import javax.annotation.Nullable;
import java.util.function.Supplier;

import static com.google.common.base.Strings.isNullOrEmpty;

public class Requirements {

    public static void requireArgument(boolean condition, String message) {
        if (!condition) {
            throw new ArgumentException(message);
        }
    }

    public static void requireArgument(boolean condition, Supplier<Throwable> supplier) {
        if (!condition) {
            pleaseThrow(supplier.get());
        }
    }

    public static <T> T requireNotNullArgument(@Nullable T argument, String message) {
        requireArgument(argument != null, message);
        return argument;
    }

    public static String requireNotNullAndNotEmptyArgument(@Nullable String argument, String message) {
        requireArgument(!isNullOrEmpty(argument), message);
        return argument;
    }

    public static String requireNotNullAndNotBlankArgument(@Nullable String argument, String message) {
        requireArgument(!isNullOrEmpty(argument) && !argument.trim().isEmpty(), message);
        return argument;
    }

    public static <T> T requireNotNullArgument(@Nullable T argument, Supplier<Throwable> supplier) {
        requireArgument(argument != null, supplier);
        return argument;
    }

    public static String requireNotNullAndNotEmptyArgument(@Nullable String argument, Supplier<Throwable> supplier) {
        requireArgument(!isNullOrEmpty(argument), supplier);
        return argument;
    }

    public static String requireNotNullAndNotBlankArgument(@Nullable String argument, Supplier<Throwable> supplier) {
        requireArgument(!isNullOrEmpty(argument) && !argument.trim().isEmpty(), supplier);
        return argument;
    }

    public static void requireState(boolean condition, String message) {
        if (!condition) {
            throw new StateException(message);
        }
    }

    public static void requireState(boolean condition, Supplier<Throwable> supplier) {
        if (!condition) {
            pleaseThrow(supplier.get());
        }
    }

    public static <T> T requireNotNullState(@Nullable T state, String message) {
        requireState(state != null, message);
        return state;
    }

    public static String requireNotNullAndNotEmptyState(@Nullable String state, String message) {
        requireState(!isNullOrEmpty(state), message);
        return state;
    }

    public static String requireNotNullAndNotBlankState(@Nullable String state, String message) {
        requireState(!isNullOrEmpty(state) && !state.trim().isEmpty(), message);
        return state;
    }

    public static <T> T requireNotNullState(@Nullable T state, Supplier<Throwable> supplier) {
        requireState(state != null, supplier);
        return state;
    }

    public static String requireNotNullAndNotEmptyState(@Nullable String state, Supplier<Throwable> supplier) {
        requireState(!isNullOrEmpty(state), supplier);
        return state;
    }

    public static String requireNotNullAndNotBlankState(@Nullable String state, Supplier<Throwable> supplier) {
        requireState(!isNullOrEmpty(state) && !state.trim().isEmpty(), supplier);
        return state;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void pleaseThrow(Throwable e) throws T {
        throw (T) e;
    }

    private Requirements() {
    }
}
