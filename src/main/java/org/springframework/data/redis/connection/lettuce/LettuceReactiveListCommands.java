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

import java.nio.ByteBuffer;
import java.util.List;

import org.reactivestreams.Publisher;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ReactiveListCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ByteBufferResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.RangeCommand;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public class LettuceReactiveListCommands implements ReactiveListCommands {

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveListCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	public LettuceReactiveListCommands(LettuceReactiveRedisConnection connection) {

		Assert.notNull(connection, "Connection must not be null!");
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#rPush(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<PushCommand, Long>> rPush(Publisher<PushCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {

				if (!command.getUpsert() && command.getValues().size() > 1) {
					throw new InvalidDataAccessApiUsageException("RPUSHX only allows one value!");
				}

				byte[][] values = command.getValues().stream().map(ByteBuffer::array).toArray(size -> new byte[size][]);

				return LettuceReactiveRedisConnection.<Long> monoConverter().convert(command.getUpsert()
						? cmd.rpush(command.getKey().array(), values) : cmd.rpushx(command.getKey().array(), values[0]))
						.map(value -> new NumericResponse<>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#lPush(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<PushCommand, Long>> lPush(Publisher<PushCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {

				if (!command.getUpsert() && command.getValues().size() > 1) {
					throw new InvalidDataAccessApiUsageException("LPUSHX only allows one value!");
				}

				byte[][] values = command.getValues().stream().map(ByteBuffer::array).toArray(size -> new byte[size][]);

				return LettuceReactiveRedisConnection.<Long> monoConverter().convert(command.getUpsert()
						? cmd.lpush(command.getKey().array(), values) : cmd.lpushx(command.getKey().array(), values[0]))
						.map(value -> new NumericResponse<>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#lLen(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<KeyCommand, Long>> lLen(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {
				return LettuceReactiveRedisConnection.<Long> monoConverter().convert(cmd.llen(command.getKey().array()))
						.map(value -> new NumericResponse<>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#lRange(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<RangeCommand, ByteBuffer>> lRange(Publisher<RangeCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {
				return LettuceReactiveRedisConnection.<List<ByteBuffer>> monoConverter()
						.convert(cmd.lrange(command.getKey().array(), command.getRange().getLowerBound(),
								command.getRange().getUpperBound()).map(ByteBuffer::wrap).toList())
						.map(value -> new MultiValueResponse<RangeCommand, ByteBuffer>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#lTrim(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<RangeCommand>> lTrim(Publisher<RangeCommand> commands) {
		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {
				return LettuceReactiveRedisConnection.<Boolean> monoConverter()
						.convert(cmd
								.ltrim(command.getKey().array(), command.getRange().getLowerBound(), command.getRange().getUpperBound())
								.map(LettuceConverters::stringToBoolean))
						.map(value -> new BooleanResponse<>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#lIndex(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<ByteBufferResponse<LIndexCommand>> lIndex(Publisher<LIndexCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {
				return LettuceReactiveRedisConnection.<ByteBuffer> monoConverter()
						.convert(cmd.lindex(command.getKey().array(), command.getIndex()).map(ByteBuffer::wrap))
						.map(value -> new ByteBufferResponse<>(command, value));
			});
		});
	}
}
