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
package org.springframework.data.redis.connection.lettuce;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.ReactiveZSetCommands;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.lambdaworks.redis.ScoredValue;
import com.lambdaworks.redis.ZAddArgs;

import reactor.core.publisher.Flux;
import rx.Observable;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public class LettuceReactiveZSetCommands implements ReactiveZSetCommands {

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveSetCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	public LettuceReactiveZSetCommands(LettuceReactiveRedisConnection connection) {

		Assert.notNull(connection, "Connection must not be null!");
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zAdd(org.reactivestreams.Publisher)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Flux<NumericResponse<ZAddCommand, Long>> zAdd(Publisher<ZAddCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {

				ZAddArgs args = null;

				if (command.getIncr() != null || command.getUpsert() != null || command.getReturnTotalChanged() != null) {

					if (ObjectUtils.nullSafeEquals(command.getIncr(), Boolean.TRUE)) {

						if (command.getTuples().size() > 1) {
							throw new IllegalArgumentException("ZADD INCR must not contain more than one tuple.");
						}

						// args = ZAddArgs.Builder.incr();
						throw new IllegalArgumentException("Lettuce does not yet implement INCR ¯\\_(ツ)_/¯");
					}

					if (ObjectUtils.nullSafeEquals(command.getReturnTotalChanged(), Boolean.TRUE)) {
						args = ZAddArgs.Builder.ch();
					}

					if (command.getUpsert() != null) {

						if (command.getUpsert().equals(Boolean.TRUE)) {
							args = ZAddArgs.Builder.nx();
						} else {
							args = ZAddArgs.Builder.xx();
						}
					}
				}

				ScoredValue<byte[]>[] values = command.getTuples().stream()
						.map(tuple -> new ScoredValue<byte[]>(tuple.getScore(), tuple.getValue()))
						.toArray(size -> new ScoredValue[size]);

				Observable<Long> result = args == null ? cmd.zadd(command.getKey().array(), values)
						: cmd.zadd(command.getKey().array(), args, values);

				return LettuceReactiveRedisConnection.<Long> monoConverter().convert(result)
						.map(value -> new NumericResponse<>(command, value));
			});
		});
	}
}
