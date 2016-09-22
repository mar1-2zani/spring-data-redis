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
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.RangeCommand;
import org.springframework.data.redis.connection.RedisListCommands.Position;
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

	/**
	 * @author Christoph Strobl
	 */
	public class LIndexCommand extends KeyCommand {

		private final Long index;

		private LIndexCommand(ByteBuffer key, Long index) {

			super(key);
			this.index = index;
		}

		public static LIndexCommand elementAt(Long index) {
			return new LIndexCommand(null, index);
		}

		public LIndexCommand from(ByteBuffer key) {
			return new LIndexCommand(key, index);
		}

		public Long getIndex() {
			return index;
		}
	}

	/**
	 * Get element at {@code index} form list at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param index
	 * @return
	 */
	default Mono<ByteBuffer> lIndex(ByteBuffer key, long index) {

		try {
			Assert.notNull(key, "key must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return lIndex(Mono.just(LIndexCommand.elementAt(index).from(key))).next().map(ByteBufferResponse::getOutput);
	}

	/**
	 * Get element at {@link LIndexCommand#getIndex()} form list at {@link LIndexCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<ByteBufferResponse<LIndexCommand>> lIndex(Publisher<LIndexCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	public class LInsertCommand extends KeyCommand {

		private final Position position;
		private final ByteBuffer pivot;
		private final ByteBuffer value;

		public LInsertCommand(ByteBuffer key, Position position, ByteBuffer pivot, ByteBuffer value) {

			super(key);
			this.position = position;
			this.pivot = pivot;
			this.value = value;
		}

		public static LInsertCommand value(ByteBuffer value) {
			return new LInsertCommand(null, null, null, value);
		}

		public LInsertCommand before(ByteBuffer pivot) {
			return new LInsertCommand(getKey(), Position.BEFORE, pivot, value);
		}

		public LInsertCommand after(ByteBuffer pivot) {
			return new LInsertCommand(getKey(), Position.AFTER, pivot, value);
		}

		public LInsertCommand forKey(ByteBuffer key) {
			return new LInsertCommand(key, position, pivot, value);
		}

		public ByteBuffer getValue() {
			return value;
		}

		public Position getPosition() {
			return position;
		}

		public ByteBuffer getPivot() {
			return pivot;
		}
	}

	/**
	 * Insert {@code value} {@link Position#BEFORE} or {@link Position#AFTER} existing {@code pivot} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> lInsert(ByteBuffer key, Position position, ByteBuffer pivot, ByteBuffer value) {

		try {
			Assert.notNull(key, "key must not be null!");
			Assert.notNull(position, "position must not be null!");
			Assert.notNull(pivot, "pivot must not be null!");
			Assert.notNull(value, "Value must not be null!");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		LInsertCommand command = LInsertCommand.value(value);
		command = Position.BEFORE.equals(position) ? command.before(pivot) : command.after(pivot);
		command = command.forKey(key);
		return lInsert(Mono.just(command)).next().map(NumericResponse::getOutput);
	}

	/**
	 * Insert {@link LInsertCommand#getValue()} {@link Position#BEFORE} or {@link Position#AFTER} existing
	 * {@link LInsertCommand#getPivot()} for {@link LInsertCommand#getKey()}
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<LInsertCommand, Long>> lInsert(Publisher<LInsertCommand> commands);

}
