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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.reactivestreams.Publisher;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveGeoCommands {

	/**
	 * @author Christoph Strobl
	 */
	public class GeoAddCommand extends KeyCommand {

		private final List<GeoLocation<ByteBuffer>> geoLocations;

		public GeoAddCommand(ByteBuffer key, List<GeoLocation<ByteBuffer>> geoLocations) {

			super(key);
			this.geoLocations = geoLocations;
		}

		public static GeoAddCommand location(GeoLocation<ByteBuffer> geoLocation) {
			return new GeoAddCommand(null, Collections.singletonList(geoLocation));
		}

		public static GeoAddCommand locations(List<GeoLocation<ByteBuffer>> geoLocations) {
			return new GeoAddCommand(null, new ArrayList<>(geoLocations));
		}

		public GeoAddCommand to(ByteBuffer key) {
			return new GeoAddCommand(key, geoLocations);
		}

		public List<GeoLocation<ByteBuffer>> getGeoLocations() {
			return geoLocations;
		}

	}

	/**
	 * Add {@link Point} with given {@literal member} to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param point must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> geoAdd(ByteBuffer key, Point point, ByteBuffer member) {

		try {
			Assert.notNull(key, "key must not be null");
			Assert.notNull(point, "point must not be null");
			Assert.notNull(member, "member must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return geoAdd(key, new GeoLocation<ByteBuffer>(member, point));
	}

	/**
	 * Add {@link GeoLocation} to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param location must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> geoAdd(ByteBuffer key, GeoLocation<ByteBuffer> location) {

		try {
			Assert.notNull(key, "key must not be null");
			Assert.notNull(location, "location must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return geoAdd(key, Collections.singletonList(location));
	}

	/**
	 * Add {@link GeoLocation} to {@literal key}.
	 * 
	 * @param key must not be {@literal null}.
	 * @param locations must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> geoAdd(ByteBuffer key, List<GeoLocation<ByteBuffer>> locations) {

		try {
			Assert.notNull(key, "key must not be null");
			Assert.notNull(locations, "locations must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return geoAdd(Mono.just(GeoAddCommand.locations(locations).to(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Add {@link GeoLocation}s to {@literal key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<GeoAddCommand, Long>> geoAdd(Publisher<GeoAddCommand> commands);

}
