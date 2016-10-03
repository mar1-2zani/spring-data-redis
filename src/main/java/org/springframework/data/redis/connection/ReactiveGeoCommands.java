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
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit;
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

	/**
	 * @author Christoph Strobl
	 */
	public class GeoDistCommand extends KeyCommand {

		private final ByteBuffer from;
		private final ByteBuffer to;
		private final Metric metric;

		private GeoDistCommand(ByteBuffer key, ByteBuffer from, ByteBuffer to, Metric metric) {
			super(key);
			this.from = from;
			this.to = to;
			this.metric = metric;
		}

		static GeoDistCommand units(Metric unit) {
			return new GeoDistCommand(null, null, null, unit);
		}

		public static GeoDistCommand meters() {
			return units(DistanceUnit.METERS);
		}

		public static GeoDistCommand kiometers() {
			return units(DistanceUnit.KILOMETERS);
		}

		public static GeoDistCommand miles() {
			return units(DistanceUnit.MILES);
		}

		public static GeoDistCommand feet() {
			return units(DistanceUnit.FEET);
		}

		public GeoDistCommand between(ByteBuffer from) {
			return new GeoDistCommand(getKey(), from, to, metric);
		}

		public GeoDistCommand and(ByteBuffer to) {
			return new GeoDistCommand(getKey(), from, to, metric);
		}

		public GeoDistCommand forKey(ByteBuffer key) {
			return new GeoDistCommand(key, from, to, metric);
		}

		public ByteBuffer from() {
			return from;
		}

		public ByteBuffer to() {
			return to;
		}

		public Metric getMetric() {
			return metric;
		}
	}

	/**
	 * Get the {@link Distance} between {@literal from} and {@literal to}.
	 *
	 * @param key must not be {@literal null}.
	 * @param from must not be {@literal null}.
	 * @param to must not be {@literal null}.
	 * @return
	 */
	default Mono<Distance> geoDist(ByteBuffer key, ByteBuffer from, ByteBuffer to) {
		return geoDist(key, from, to, null);
	}

	/**
	 * Get the {@link Distance} between {@literal from} and {@literal to}.
	 *
	 * @param key must not be {@literal null}.
	 * @param from must not be {@literal null}.
	 * @param to must not be {@literal null}.
	 * @param metric can be {@literal null} and defaults to {@link DistanceUnit#METERS}.
	 * @return
	 */
	default Mono<Distance> geoDist(ByteBuffer key, ByteBuffer from, ByteBuffer to, Metric metric) {

		try {
			Assert.notNull(key, "key must not be null");
			Assert.notNull(from, "from must not be null");
			Assert.notNull(to, "to must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return geoDist(Mono.just(GeoDistCommand.units(metric).between(from).and(to).forKey(key))).next()
				.map(CommandResponse::getOutput);
	}

	/**
	 * Get the {@link Distance} between {@literal from} and {@literal to}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<CommandResponse<GeoDistCommand, Distance>> geoDist(Publisher<GeoDistCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	public class GeoHashCommand extends KeyCommand {

		private final List<ByteBuffer> members;

		private GeoHashCommand(ByteBuffer key, List<ByteBuffer> members) {

			super(key);
			this.members = members;
		}

		public static GeoHashCommand member(ByteBuffer member) {
			return new GeoHashCommand(null, Collections.singletonList(member));
		}

		public static GeoHashCommand members(List<ByteBuffer> members) {
			return new GeoHashCommand(null, new ArrayList<>(members));
		}

		public GeoHashCommand of(ByteBuffer key) {
			return new GeoHashCommand(key, members);
		}

		public List<ByteBuffer> getMembers() {
			return members;
		}
	}

	/**
	 * Get geohash representation of the position for the one {@literal member}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @return
	 */
	default Mono<String> geoHash(ByteBuffer key, ByteBuffer member) {

		try {
			Assert.notNull(member, "member must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return geoHash(key, Collections.singletonList(member)).map(vals -> vals.isEmpty() ? null : vals.iterator().next());
	}

	/**
	 * Get geohash representation of the position for one or more {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return
	 */
	default Mono<List<String>> geoHash(ByteBuffer key, List<ByteBuffer> members) {

		try {
			Assert.notNull(key, "key must not be null");
			Assert.notNull(members, "members must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return geoHash(Mono.just(GeoHashCommand.members(members).of(key))).next().map(MultiValueResponse::getOutput);
	}

	/**
	 * Get geohash representation of the position for one or more {@literal member}s.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<MultiValueResponse<GeoHashCommand, String>> geoHash(Publisher<GeoHashCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	public class GeoPosCommand extends KeyCommand {

		private final List<ByteBuffer> members;

		private GeoPosCommand(ByteBuffer key, List<ByteBuffer> members) {

			super(key);
			this.members = members;
		}

		public static GeoPosCommand member(ByteBuffer member) {
			return new GeoPosCommand(null, Collections.singletonList(member));
		}

		public static GeoPosCommand members(List<ByteBuffer> members) {
			return new GeoPosCommand(null, new ArrayList<>(members));
		}

		public GeoPosCommand of(ByteBuffer key) {
			return new GeoPosCommand(key, members);
		}

		public List<ByteBuffer> getMembers() {
			return members;
		}
	}

	/**
	 * Get the {@link Point} representation of positions for the {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @return
	 */
	default Mono<Point> geoPos(ByteBuffer key, ByteBuffer member) {

		try {
			Assert.notNull(member, "member must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return geoPos(key, Collections.singletonList(member)).map(vals -> vals.isEmpty() ? null : vals.iterator().next());
	}

	/**
	 * Get the {@link Point} representation of positions for one or more {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return
	 */
	default Mono<List<Point>> geoPos(ByteBuffer key, List<ByteBuffer> members) {

		try {
			Assert.notNull(key, "key must not be null");
			Assert.notNull(members, "members must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return geoPos(Mono.just(GeoPosCommand.members(members).of(key))).next().map(MultiValueResponse::getOutput);
	}

	/**
	 * Get the {@link Point} representation of positions for one or more {@literal member}s.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<MultiValueResponse<GeoPosCommand, Point>> geoPos(Publisher<GeoPosCommand> commands);

}
