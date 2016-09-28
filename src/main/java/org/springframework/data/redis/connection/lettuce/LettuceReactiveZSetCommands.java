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
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.redis.connection.DefaultTuple;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.ReactiveZSetCommands;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
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
	public Flux<NumericResponse<ZAddCommand, Number>> zAdd(Publisher<ZAddCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {

				ZAddArgs args = null;

				if (command.getIncr() != null || command.getUpsert() != null || command.getReturnTotalChanged() != null) {

					if (ObjectUtils.nullSafeEquals(command.getIncr(), Boolean.TRUE)) {

						if (command.getTuples().size() > 1) {
							throw new IllegalArgumentException("ZADD INCR must not contain more than one tuple.");
						}

						Tuple tuple = command.getTuples().iterator().next();

						return LettuceReactiveRedisConnection.<Double> monoConverter()
								.convert(cmd.zaddincr(command.getKey().array(), tuple.getScore(), tuple.getValue()))
								.map(value -> new NumericResponse<>(command, value));
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zRem(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ZRemCommand, Long>> zRem(Publisher<ZRemCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {
				return LettuceReactiveRedisConnection.<Long> monoConverter()
						.convert(cmd.zrem(command.getKey().array(),
								command.getValues().stream().map(ByteBuffer::array).toArray(size -> new byte[size][])))
						.map(value -> new NumericResponse<>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zIncrBy(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ZIncrByCommand, Double>> zIncrBy(Publisher<ZIncrByCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {
				return LettuceReactiveRedisConnection.<Double> monoConverter()
						.convert(
								cmd.zincrby(command.getKey().array(), command.getIncrement().doubleValue(), command.getValue().array()))
						.map(value -> new NumericResponse<>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zRank(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ZRankCommand, Long>> zRank(Publisher<ZRankCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {

				Observable<Long> result = ObjectUtils.nullSafeEquals(command.getDirection(), Direction.ASC)
						? cmd.zrank(command.getKey().array(), command.getValue().array())
						: cmd.zrevrank(command.getKey().array(), command.getValue().array());

				return LettuceReactiveRedisConnection.<Long> monoConverter().convert(result)
						.map(value -> new NumericResponse<>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zRange(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<ZRangeCommand, Tuple>> zRange(Publisher<ZRangeCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {

				Observable<List<Tuple>> result = null;

				if (ObjectUtils.nullSafeEquals(command.getDirection(), Direction.ASC)) {
					if (ObjectUtils.nullSafeEquals(command.getWithScores(), Boolean.TRUE)) {

						result = cmd
								.zrangeWithScores(command.getKey().array(), command.getRange().getLowerBound(),
										command.getRange().getUpperBound())
								.map(sc -> (Tuple) new DefaultTuple(sc.value, sc.score)).toList();

					} else {

						result = cmd
								.zrange(command.getKey().array(), command.getRange().getLowerBound(),
										command.getRange().getUpperBound())
								.map(value -> (Tuple) new DefaultTuple(value, Double.NaN)).toList();
					}
				}

				else {
					if (ObjectUtils.nullSafeEquals(command.getWithScores(), Boolean.TRUE)) {
						result = cmd
								.zrevrangeWithScores(command.getKey().array(), command.getRange().getLowerBound(),
										command.getRange().getUpperBound())
								.map(sc -> (Tuple) new DefaultTuple(sc.value, sc.score)).toList();

					} else {

						result = cmd
								.zrevrange(command.getKey().array(), command.getRange().getLowerBound(),
										command.getRange().getUpperBound())
								.map(value -> (Tuple) new DefaultTuple(value, Double.NaN)).toList();
					}
				}

				return LettuceReactiveRedisConnection.<List<Tuple>> monoConverter().convert(result)
						.map(value -> new MultiValueResponse<>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zRange(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<ZRangeByScoreCommand, Tuple>> zRangeByScore(Publisher<ZRangeByScoreCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {

				Observable<List<Tuple>> result = null;

				if (ObjectUtils.nullSafeEquals(command.getDirection(), Direction.ASC)) {
					if (ObjectUtils.nullSafeEquals(command.getWithScores(), Boolean.TRUE)) {

						result = cmd
								.zrangebyscoreWithScores(command.getKey().array(), command.getRange().getLowerBound(),
										command.getRange().getUpperBound())
								.map(sc -> (Tuple) new DefaultTuple(sc.value, sc.score)).toList();

					} else {

						result = cmd
								.zrangebyscore(command.getKey().array(), command.getRange().getLowerBound(),
										command.getRange().getUpperBound())
								.map(value -> (Tuple) new DefaultTuple(value, Double.NaN)).toList();
					}
				}

				else {
					if (ObjectUtils.nullSafeEquals(command.getWithScores(), Boolean.TRUE)) {
						result = cmd
								.zrevrangebyscoreWithScores(command.getKey().array(), command.getRange().getLowerBound(),
										command.getRange().getUpperBound())
								.map(sc -> (Tuple) new DefaultTuple(sc.value, sc.score)).toList();

					} else {

						result = cmd
								.zrevrangebyscore(command.getKey().array(), command.getRange().getLowerBound(),
										command.getRange().getUpperBound())
								.map(value -> (Tuple) new DefaultTuple(value, Double.NaN)).toList();
					}
				}

				return LettuceReactiveRedisConnection.<List<Tuple>> monoConverter().convert(result)
						.map(value -> new MultiValueResponse<>(command, value));
			});
		});
	}

}
