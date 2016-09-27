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
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveZSetCommands {

	/**
	 * @author Christoph Strobl
	 */
	public class ZAddCommand extends KeyCommand {

		private final List<Tuple> tuples;
		private final Boolean upsert;
		private final Boolean returnTotalChanged;
		private final Boolean incr;

		private ZAddCommand(ByteBuffer key, List<Tuple> tuples, Boolean upsert, Boolean returnTotalChanged, Boolean incr) {

			super(key);
			this.tuples = tuples;
			this.upsert = upsert;
			this.returnTotalChanged = returnTotalChanged;
			this.incr = incr;
		}

		public static ZAddCommand tuple(Tuple tuple) {
			return tuples(Collections.singletonList(tuple));
		}

		public static ZAddCommand tuples(List<Tuple> tuples) {
			return new ZAddCommand(null, tuples, null, null, null);
		}

		public ZAddCommand to(ByteBuffer key) {
			return new ZAddCommand(key, tuples, upsert, returnTotalChanged, incr);
		}

		public ZAddCommand xx() {
			return new ZAddCommand(getKey(), tuples, false, returnTotalChanged, incr);
		}

		public ZAddCommand nx() {
			return new ZAddCommand(getKey(), tuples, true, returnTotalChanged, incr);
		}

		public ZAddCommand ch() {
			return new ZAddCommand(getKey(), tuples, upsert, true, incr);
		}

		public ZAddCommand incr() {
			return new ZAddCommand(getKey(), tuples, upsert, upsert, true);
		}

		public List<Tuple> getTuples() {
			return tuples;
		}

		public Boolean getUpsert() {
			return upsert;
		}

		public Boolean getIncr() {
			return incr;
		}

		public Boolean getReturnTotalChanged() {
			return returnTotalChanged;
		}
	}

	/**
	 * Add {@code value} to a sorted set at {@code key}, or update its {@code score} if it already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param score must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> zAdd(ByteBuffer key, Double score, ByteBuffer value) {

		try {
			Assert.notNull(key, "key must not be null");
			Assert.notNull(score, "score must not be null");
			Assert.notNull(value, "value must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return zAdd(Mono.just(ZAddCommand.tuple(new DefaultTuple(value.array(), score)).to(key))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Add {@link ZAddCommand#getTuple()} to a sorted set at {@link ZAddCommand#getKey()}, or update its {@code score} if
	 * it already exists.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<ZAddCommand, Long>> zAdd(Publisher<ZAddCommand> commands);

}
