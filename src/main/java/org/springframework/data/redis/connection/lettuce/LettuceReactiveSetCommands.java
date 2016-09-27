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

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ByteBufferResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.ReactiveSetCommands;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;

/**
 * @author Christoph Strobl
 */
public class LettuceReactiveSetCommands implements ReactiveSetCommands {

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveSetCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	public LettuceReactiveSetCommands(LettuceReactiveRedisConnection connection) {

		Assert.notNull(connection, "Connection must not be null!");
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSetCommands#sAdd(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<SAddCommand, Long>> sAdd(Publisher<SAddCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {

				return LettuceReactiveRedisConnection.<Long> monoConverter()
						.convert(cmd.sadd(command.getKey().array(),
								command.getValues().stream().map(ByteBuffer::array).toArray(size -> new byte[size][])))
						.map(value -> new NumericResponse<SAddCommand, Long>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSetCommands#sRem(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<SRemCommand, Long>> sRem(Publisher<SRemCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {

				return LettuceReactiveRedisConnection.<Long> monoConverter()
						.convert(cmd.srem(command.getKey().array(),
								command.getValues().stream().map(ByteBuffer::array).toArray(size -> new byte[size][])))
						.map(value -> new NumericResponse<SRemCommand, Long>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSetCommands#sPop(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<ByteBufferResponse<KeyCommand>> sPop(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {
				return LettuceReactiveRedisConnection.<ByteBuffer> monoConverter()
						.convert(cmd.spop(command.getKey().array()).map(ByteBuffer::wrap))
						.map(value -> new ByteBufferResponse<>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSetCommands#sMove(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<SMoveCommand>> sMove(Publisher<SMoveCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {

				return LettuceReactiveRedisConnection.<Boolean> monoConverter()
						.convert(cmd.smove(command.getKey().array(), command.getDestination().array(), command.getValue().array()))
						.map(value -> new BooleanResponse<>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSetCommands#sCard(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<KeyCommand, Long>> sCard(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {
				return LettuceReactiveRedisConnection.<Long> monoConverter().convert(cmd.scard(command.getKey().array()))
						.map(value -> new NumericResponse<>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSetCommands#sIsMember(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<SIsMemberCommand>> sIsMember(Publisher<SIsMemberCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {
				return LettuceReactiveRedisConnection.<Boolean> monoConverter()
						.convert(cmd.sismember(command.getKey().array(), command.getValue().array()))
						.map(value -> new BooleanResponse<>(command, value));
			});
		});
	}
}
