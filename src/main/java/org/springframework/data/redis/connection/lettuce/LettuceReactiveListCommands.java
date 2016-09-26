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
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
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
import org.springframework.data.redis.connection.RedisListCommands.Position;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import reactor.core.publisher.Flux;
import rx.Observable;

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
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#lPush(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<PushCommand, Long>> push(Publisher<PushCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {

				if (!command.getUpsert() && command.getValues().size() > 1) {
					throw new InvalidDataAccessApiUsageException(
							String.format("%s PUSHX only allows one value!", command.getDirection()));
				}

				byte[][] values = command.getValues().stream().map(ByteBuffer::array).toArray(size -> new byte[size][]);

				Observable<Long> pushResult = null;

				if (ObjectUtils.nullSafeEquals(Direction.RIGHT, command.getDirection())) {
					pushResult = command.getUpsert() ? cmd.rpush(command.getKey().array(), values)
							: cmd.rpushx(command.getKey().array(), values[0]);
				} else {
					pushResult = command.getUpsert() ? cmd.lpush(command.getKey().array(), values)
							: cmd.lpushx(command.getKey().array(), values[0]);
				}

				return LettuceReactiveRedisConnection.<Long> monoConverter().convert(pushResult)
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#lInsert(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<LInsertCommand, Long>> lInsert(Publisher<LInsertCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {
				return LettuceReactiveRedisConnection.<Long> monoConverter()
						.convert(cmd.linsert(command.getKey().array(), Position.BEFORE.equals(command.getPosition()),
								command.getPivot().array(), command.getValue().array()))
						.map(value -> new NumericResponse<>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#lSet(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<LSetCommand>> lSet(Publisher<LSetCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {
				return LettuceReactiveRedisConnection.<Boolean> monoConverter()
						.convert(cmd.lset(command.getKey().array(), command.getIndex(), command.getValue().array())
								.map(LettuceConverters::stringToBoolean))
						.map(value -> new BooleanResponse<>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#lRem(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<LRemCommand, Long>> lRem(Publisher<LRemCommand> commands) {
		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {
				return LettuceReactiveRedisConnection.<Long> monoConverter()
						.convert(cmd.lrem(command.getKey().array(), command.getCount(), command.getValue().array()))
						.map(value -> new NumericResponse<>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#rPop(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<ByteBufferResponse<PopCommand>> pop(Publisher<PopCommand> commands) {
		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {

				Observable<byte[]> popResult = ObjectUtils.nullSafeEquals(Direction.RIGHT, command.getDirection())
						? cmd.rpop(command.getKey().array()) : cmd.lpop(command.getKey().array());

				return LettuceReactiveRedisConnection.<ByteBuffer> monoConverter().convert(popResult.map(ByteBuffer::wrap))
						.map(value -> new ByteBufferResponse<>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#bPop(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<PopResponse> bPop(Publisher<BPopCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {

				byte[][] keys = command.getKeys().stream().map(ByteBuffer::array).toArray(size -> new byte[size][]);
				long timeout = command.getTimeout().get(ChronoUnit.SECONDS);

				Observable<PopResult> mappedObservable = (ObjectUtils.nullSafeEquals(Direction.RIGHT, command.getDirection())
						? cmd.brpop(timeout, keys) : cmd.blpop(timeout, keys))
								.map(kv -> Arrays.asList(ByteBuffer.wrap(kv.key), ByteBuffer.wrap(kv.value)))
								.map(val -> new PopResult(val));

				return LettuceReactiveRedisConnection.<PopResult> monoConverter().convert(mappedObservable)
						.map(value -> new PopResponse(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#rPopLPush(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<ByteBufferResponse<RPopLPushCommand>> rPopLPush(Publisher<RPopLPushCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {
				return LettuceReactiveRedisConnection.<ByteBuffer> monoConverter()
						.convert(cmd.rpoplpush(command.getKey().array(), command.getDestination().array()).map(ByteBuffer::wrap))
						.map(value -> new ByteBufferResponse<>(command, value));
			});
		});
	}

}
