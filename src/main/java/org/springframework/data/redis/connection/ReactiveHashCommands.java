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
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveHashCommands {

	/**
	 * @author Christoph Strobl
	 */
	public class HSetCommand extends KeyCommand {

		private final ByteBuffer field;
		private final ByteBuffer value;
		private final Boolean upsert;

		private HSetCommand(ByteBuffer key, ByteBuffer field, ByteBuffer value, Boolean upsert) {

			super(key);
			this.field = field;
			this.value = value;
			this.upsert = upsert;
		}

		public static HSetCommand value(ByteBuffer value) {
			return new HSetCommand(null, null, value, Boolean.TRUE);
		}

		public HSetCommand ofField(ByteBuffer field) {
			return new HSetCommand(getKey(), field, value, upsert);
		}

		public HSetCommand forKey(ByteBuffer key) {
			return new HSetCommand(key, field, value, upsert);
		}

		public HSetCommand ifValueNotExists() {
			return new HSetCommand(getKey(), field, value, Boolean.FALSE);
		}

		public ByteBuffer getField() {
			return field;
		}

		public ByteBuffer getValue() {
			return value;
		}

		public Boolean isUpsert() {
			return upsert;
		}
	}

	/**
	 * Set the {@code value} of a hash {@code field}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> hSet(ByteBuffer key, ByteBuffer field, ByteBuffer value) {

		try {
			Assert.notNull(key, "key must not be null");
			Assert.notNull(field, "field must not be null");
			Assert.notNull(value, "value must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return hSet(Mono.just(HSetCommand.value(value).ofField(field).forKey(key))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Set the {@code value} of a hash {@code field}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> hSetNX(ByteBuffer key, ByteBuffer field, ByteBuffer value) {

		try {
			Assert.notNull(key, "key must not be null");
			Assert.notNull(field, "field must not be null");
			Assert.notNull(value, "value must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return hSet(Mono.just(HSetCommand.value(value).ofField(field).forKey(key).ifValueNotExists())).next()
				.map(BooleanResponse::getOutput);
	}

	/**
	 * Set the {@code value} of a hash {@code field}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<BooleanResponse<HSetCommand>> hSet(Publisher<HSetCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	public class HGetCommand extends KeyCommand {

		private List<ByteBuffer> fields;

		private HGetCommand(ByteBuffer key, List<ByteBuffer> fields) {

			super(key);
			this.fields = fields;
		}

		public static HGetCommand field(ByteBuffer field) {
			return new HGetCommand(null, Collections.singletonList(field));
		}

		public static HGetCommand fields(List<ByteBuffer> fields) {
			return new HGetCommand(null, new ArrayList<>(fields));
		}

		public HGetCommand from(ByteBuffer key) {
			return new HGetCommand(key, fields);
		}

		public List<ByteBuffer> getFields() {
			return fields;
		}
	}

	/**
	 * Get value for given {@code field} from hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return
	 */
	default Mono<ByteBuffer> hGet(ByteBuffer key, ByteBuffer field) {
		return hMGet(key, Collections.singletonList(field)).map(val -> val.isEmpty() ? null : val.iterator().next());
	}

	/**
	 * Get values for given {@code fields} from hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return
	 */
	default Mono<List<ByteBuffer>> hMGet(ByteBuffer key, List<ByteBuffer> fields) {

		try {
			Assert.notNull(key, "key must not be null");
			Assert.notNull(fields, "fields must not be null");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return hMGet(Mono.just(HGetCommand.fields(fields).from(key))).next().map(MultiValueResponse::getOutput);
	}

	/**
	 * Get values for given {@code fields} from hash at {@code key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<MultiValueResponse<HGetCommand, ByteBuffer>> hMGet(Publisher<HGetCommand> commands);

}
