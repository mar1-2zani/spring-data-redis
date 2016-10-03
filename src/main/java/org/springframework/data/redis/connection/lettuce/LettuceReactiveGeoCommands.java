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
import java.util.ArrayList;
import java.util.List;

import org.reactivestreams.Publisher;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.ReactiveGeoCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.util.Assert;

import com.lambdaworks.redis.GeoArgs;

import reactor.core.publisher.Flux;
import rx.Observable;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public class LettuceReactiveGeoCommands implements ReactiveGeoCommands {

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveGeoCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	public LettuceReactiveGeoCommands(LettuceReactiveRedisConnection connection) {

		Assert.notNull(connection, "Connection must not be null!");
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveGeoCommands#geoAdd(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<GeoAddCommand, Long>> geoAdd(Publisher<GeoAddCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {

				List<Object> values = new ArrayList<Object>();
				for (GeoLocation<ByteBuffer> location : command.getGeoLocations()) {

					values.add(location.getPoint().getX());
					values.add(location.getPoint().getY());
					values.add(location.getName().array());
				}

				return LettuceReactiveRedisConnection.<Long> monoConverter()
						.convert(cmd.geoadd(command.getKey().array(), values.toArray()))
						.map(value -> new NumericResponse<>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveGeoCommands#geoDist(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<CommandResponse<GeoDistCommand, Distance>> geoDist(Publisher<GeoDistCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {

				GeoArgs.Unit geoUnit = LettuceConverters.toGeoArgsUnit(command.getMetric());
				Converter<Double, Distance> distanceConverter = LettuceConverters
						.distanceConverterForMetric(command.getMetric());

				Observable<Distance> result = cmd
						.geodist(command.getKey().array(), command.from().array(), command.to().array(), geoUnit)
						.map(distanceConverter::convert);

				return LettuceReactiveRedisConnection.<Distance> monoConverter().convert(result)
						.map(value -> new CommandResponse<>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveGeoCommands#geoHash(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<GeoHashCommand, String>> geoHash(Publisher<GeoHashCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {
				return LettuceReactiveRedisConnection.<List<String>> monoConverter()
						.convert(cmd.geohash(command.getKey().array(),
								command.getMembers().stream().map(ByteBuffer::array).toArray(size -> new byte[size][])).toList())
						.map(value -> new MultiValueResponse<GeoHashCommand, String>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveGeoCommands#geoPos(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<GeoPosCommand, Point>> geoPos(Publisher<GeoPosCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {

				Observable<List<Point>> result = cmd
						.geopos(command.getKey().array(),
								command.getMembers().stream().map(ByteBuffer::array).toArray(size -> new byte[size][]))
						.map(LettuceConverters::geoCoordinatesToPoint).toList();

				return LettuceReactiveRedisConnection.<List<Point>> monoConverter().convert(result)
						.map(value -> new MultiValueResponse<>(command, value));
			});
		});
	}
}
