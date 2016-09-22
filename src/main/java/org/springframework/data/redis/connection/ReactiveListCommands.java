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
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.RangeCommand;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveListCommands {

	/**
	 * @author Christoph Strobl
	 */
	public class PushCommand extends KeyCommand {

		private List<ByteBuffer> values;
		private boolean upsert;

		private PushCommand(ByteBuffer key, List<ByteBuffer> values, boolean upsert) {
			super(key);
			this.values = values;
			this.upsert = upsert;
		}

		public static PushCommand value(ByteBuffer value) {
			return new PushCommand(null, Collections.singletonList(value), true);
		}

		public static PushCommand values(List<ByteBuffer> values) {
			return new PushCommand(null, values, true);
		}

		public PushCommand to(ByteBuffer key) {
			return new PushCommand(key, values, upsert);
		}

		public PushCommand ifExists() {
			return new PushCommand(getKey(), values, false);
		}

		public List<ByteBuffer> getValues() {
			return values;
		}

		public boolean getUpsert() {
			return upsert;
		}
	}

	/**
	 * Append {@code values} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> rPush(ByteBuffer key, List<ByteBuffer> values) {

		try {
			Assert.notNull(key, "command must not be null!");
			Assert.notNull(values, "Values must not be null!");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return rPush(Mono.just(PushCommand.values(values).to(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Append {@code values} to {@code key} only if {@code key} already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> rPushX(ByteBuffer key, ByteBuffer value) {

		try {
			Assert.notNull(key, "command must not be null!");
			Assert.notNull(value, "Value must not be null!");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return rPush(Mono.just(PushCommand.value(value).to(key).ifExists())).next().map(NumericResponse::getOutput);
	}

	/**
	 * Append {@link PushCommand#getValues()} to {@link PushCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<PushCommand, Long>> rPush(Publisher<PushCommand> commands);

	/**
	 * Prepend {@code values} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> lPush(ByteBuffer key, List<ByteBuffer> values) {

		try {
			Assert.notNull(key, "command must not be null!");
			Assert.notNull(values, "Values must not be null!");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return lPush(Mono.just(PushCommand.values(values).to(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Prepend {@code value} to {@code key} if {@code key} already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> lPushX(ByteBuffer key, ByteBuffer value) {

		try {
			Assert.notNull(key, "command must not be null!");
			Assert.notNull(value, "Value must not be null!");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return lPush(Mono.just(PushCommand.value(value).to(key).ifExists())).next().map(NumericResponse::getOutput);
	}

	/**
	 * Prepend {@link PushCommand#getValues()} to {@link PushCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<PushCommand, Long>> lPush(Publisher<PushCommand> commands);

	/**
	 * Get the size of list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> lLen(ByteBuffer key) {

		try {
			Assert.notNull(key, "key must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return lLen(Mono.just(new KeyCommand(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Get the size of list stored at {@link KeyCommand#getKey()}
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<KeyCommand, Long>> lLen(Publisher<KeyCommand> commands);

	/**
	 * Get elements between {@code begin} and {@code end} from list at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return
	 */
	default Mono<List<ByteBuffer>> lRange(ByteBuffer key, long start, long end) {

		try {
			Assert.notNull(key, "key must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return lRange(Mono.just(RangeCommand.key(key).fromIndex(start).toIndex(end))).next()
				.map(MultiValueResponse::getOutput);
	}

	/**
	 * Get elements in {@link RangeCommand#getRange()} from list at {@link RangeCommand#getKey()}
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<MultiValueResponse<RangeCommand, ByteBuffer>> lRange(Publisher<RangeCommand> commands);

	/**
	 * Trim list at {@code key} to elements between {@code begin} and {@code end}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return
	 */
	default Mono<Boolean> lTrim(ByteBuffer key, long start, long end) {

		try {
			Assert.notNull(key, "key must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return lTrim(Mono.just(RangeCommand.key(key).fromIndex(start).toIndex(end))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Trim list at {@link RangeCommand#getKey()} to elements within {@link RangeCommand#getRange()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<BooleanResponse<RangeCommand>> lTrim(Publisher<RangeCommand> commands);

}
