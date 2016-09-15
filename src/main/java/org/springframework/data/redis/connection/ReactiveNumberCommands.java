/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveNumberCommands {

	/**
	 * Increment value of {@code key} by 1.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> incr(ByteBuffer key) {

		try {
			Assert.notNull(key, "key must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}
		return incr(Mono.just(new KeyCommand(() -> key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Increment value of {@code key} by 1.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<KeyCommand, Long>> incr(Publisher<KeyCommand> keys);

	/**
	 * @author Christoph Strobl
	 */
	public class IncrByCommand<T extends Number> extends KeyCommand {

		private Supplier<T> value;

		public IncrByCommand(Supplier<ByteBuffer> key, Supplier<T> value) {
			super(key);
			this.value = value;
		}

		public static <T extends Number> ReactiveNumberCommands.IncrByCommand<T> incr(ByteBuffer key) {
			return incr(() -> key);
		}

		public static <T extends Number> ReactiveNumberCommands.IncrByCommand<T> incr(Supplier<ByteBuffer> key) {
			return new ReactiveNumberCommands.IncrByCommand<T>(key, null);
		}

		public ReactiveNumberCommands.IncrByCommand<T> by(T value) {
			return by(() -> value);
		}

		public ReactiveNumberCommands.IncrByCommand<T> by(Supplier<T> value) {
			return new ReactiveNumberCommands.IncrByCommand<T>(getKeySupplier(), value);
		}

		public T getValue() {
			return value != null ? value.get() : null;
		}

	}

	/**
	 * Increment value of {@code key} by {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default <T extends Number> Mono<T> incrBy(ByteBuffer key, T value) {

		try {
			Assert.notNull(key, "key must not be null");
			Assert.notNull(value, "value must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return incrBy(Mono.just(IncrByCommand.<T> incr(key).by(value))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Increment value of {@code key} by {@code value}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	<T extends Number> Flux<NumericResponse<ReactiveNumberCommands.IncrByCommand<T>, T>> incrBy(
			Publisher<ReactiveNumberCommands.IncrByCommand<T>> commands);

	/**
	 * @author Christoph Strobl
	 */
	public class DecrByCommand<T extends Number> extends KeyCommand {

		private Supplier<T> value;

		public DecrByCommand(Supplier<ByteBuffer> key, Supplier<T> value) {
			super(key);
			this.value = value;
		}

		public static <T extends Number> ReactiveNumberCommands.DecrByCommand<T> decr(ByteBuffer key) {
			return decr(() -> key);
		}

		public static <T extends Number> ReactiveNumberCommands.DecrByCommand<T> decr(Supplier<ByteBuffer> key) {
			return new ReactiveNumberCommands.DecrByCommand<T>(key, null);
		}

		public ReactiveNumberCommands.DecrByCommand<T> by(T value) {
			return by(() -> value);
		}

		public ReactiveNumberCommands.DecrByCommand<T> by(Supplier<T> value) {
			return new ReactiveNumberCommands.DecrByCommand<T>(getKeySupplier(), value);
		}

		public T getValue() {
			return value != null ? value.get() : null;
		}

	}

	/**
	 * Decrement value of {@code key} by 1.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> decr(ByteBuffer key) {

		try {
			Assert.notNull(key, "key must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return decr(Mono.just(new KeyCommand(() -> key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Decrement value of {@code key} by 1.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<KeyCommand, Long>> decr(Publisher<KeyCommand> keys);

	/**
	 * Decrement value of {@code key} by {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default <T extends Number> Mono<T> decrBy(ByteBuffer key, T value) {

		try {
			Assert.notNull(key, "key must not be null");
			Assert.notNull(value, "value must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return decrBy(Mono.just(DecrByCommand.<T> decr(key).by(value))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Decrement value of {@code key} by {@code value}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	<T extends Number> Flux<NumericResponse<ReactiveNumberCommands.DecrByCommand<T>, T>> decrBy(
			Publisher<ReactiveNumberCommands.DecrByCommand<T>> commands);

}
