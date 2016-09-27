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
import java.util.Collections;
import java.util.List;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ByteBufferResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveSetCommands {

	/**
	 * @author Christoph Strobl
	 */
	public class SAddCommand extends KeyCommand {

		private List<ByteBuffer> values;

		private SAddCommand(ByteBuffer key, List<ByteBuffer> values) {

			super(key);
			this.values = values;
		}

		public static SAddCommand value(ByteBuffer values) {
			return values(Collections.singletonList(values));
		}

		public static SAddCommand values(List<ByteBuffer> values) {
			return new SAddCommand(null, values);
		}

		public SAddCommand to(ByteBuffer key) {
			return new SAddCommand(key, values);
		}

		public List<ByteBuffer> getValues() {
			return values;
		}
	}

	/**
	 * Add given {@code value} to set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> sAdd(ByteBuffer key, ByteBuffer value) {

		try {
			Assert.notNull(value, "value must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return sAdd(key, Collections.singletonList(value));
	}

	/**
	 * Add given {@code values} to set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> sAdd(ByteBuffer key, List<ByteBuffer> values) {

		try {
			Assert.notNull(key, "key must not be null");
			Assert.notNull(values, "values must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return sAdd(Mono.just(SAddCommand.values(values).to(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Add given {@link SAddCommand#getValues()} to set at {@link SAddCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<SAddCommand, Long>> sAdd(Publisher<SAddCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	public class SRemCommand extends KeyCommand {

		private final List<ByteBuffer> values;

		public SRemCommand(ByteBuffer key, List<ByteBuffer> values) {

			super(key);
			this.values = values;
		}

		public static SRemCommand value(ByteBuffer values) {
			return values(Collections.singletonList(values));
		}

		public static SRemCommand values(List<ByteBuffer> values) {
			return new SRemCommand(null, values);
		}

		public SRemCommand to(ByteBuffer key) {
			return new SRemCommand(key, values);
		}

		public List<ByteBuffer> getValues() {
			return values;
		}
	}

	/**
	 * Remove given {@code value} from set at {@code key} and return the number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> sRem(ByteBuffer key, ByteBuffer value) {

		try {
			Assert.notNull(value, "value must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return sRem(key, Collections.singletonList(value));
	}

	/**
	 * Remove given {@code values} from set at {@code key} and return the number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> sRem(ByteBuffer key, List<ByteBuffer> values) {

		try {
			Assert.notNull(key, "key must not be null");
			Assert.notNull(values, "values must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return sRem(Mono.just(SRemCommand.values(values).to(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Remove given {@link SRemCommand#getValues()} from set at {@link SRemCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<SRemCommand, Long>> sRem(Publisher<SRemCommand> commands);

	/**
	 * Remove and return a random member from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<ByteBuffer> sPop(ByteBuffer key) {

		try {
			Assert.notNull(key, "key must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return sPop(Mono.just(new KeyCommand(key))).next().map(ByteBufferResponse::getOutput);
	}

	/**
	 * Remove and return a random member from set at {@link KeyCommand#getKey()}
	 *
	 * @param commands
	 * @return
	 */
	Flux<ByteBufferResponse<KeyCommand>> sPop(Publisher<KeyCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	public class SMoveCommand extends KeyCommand {

		private final ByteBuffer destination;
		private final ByteBuffer value;

		private SMoveCommand(ByteBuffer key, ByteBuffer destination, ByteBuffer value) {

			super(key);
			this.destination = destination;
			this.value = value;
		}

		public static SMoveCommand value(ByteBuffer value) {
			return new SMoveCommand(null, null, value);
		}

		public SMoveCommand from(ByteBuffer source) {
			return new SMoveCommand(source, destination, value);
		}

		public SMoveCommand to(ByteBuffer destination) {
			return new SMoveCommand(getKey(), destination, value);
		}

		public ByteBuffer getDestination() {
			return destination;
		}

		public ByteBuffer getValue() {
			return value;
		}
	}

	/**
	 * Move {@code value} from {@code sourceKey} to {@code destinationKey}
	 *
	 * @param sourceKey must not be {@literal null}.
	 * @param destinationKey must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> sMove(ByteBuffer sourceKey, ByteBuffer destinationKey, ByteBuffer value) {

		try {
			Assert.notNull(sourceKey, "sourceKey must not be null");
			Assert.notNull(destinationKey, "destinationKey must not be null");
			Assert.notNull(value, "value must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return sMove(Mono.just(SMoveCommand.value(value).from(sourceKey).to(destinationKey))).next()
				.map(BooleanResponse::getOutput);
	}

	/**
	 * Move {@link SMoveCommand#getValue()} from {@link SMoveCommand#getKey()} to {@link SMoveCommand#getDestination()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<BooleanResponse<SMoveCommand>> sMove(Publisher<SMoveCommand> commands);

	/**
	 * Get size of set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> sCard(ByteBuffer key) {

		try {
			Assert.notNull(key, "key must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return sCard(Mono.just(new KeyCommand(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Get size of set at {@link KeyCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<KeyCommand, Long>> sCard(Publisher<KeyCommand> commands);

}
