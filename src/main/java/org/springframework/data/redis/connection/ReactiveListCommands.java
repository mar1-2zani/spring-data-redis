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
import java.util.List;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveListCommands {

	/**
	 * @author Christoph Strobl
	 */
	public class PushCommand extends KeyCommand {

		private List<ByteBuffer> values;

		public PushCommand(ByteBuffer key, List<ByteBuffer> values) {
			super(key);
			this.values = values;
		}

		public static PushCommand values(List<ByteBuffer> values) {
			return new PushCommand(null, values);
		}

		public PushCommand to(ByteBuffer key) {
			return new PushCommand(key, values);
		}

		public List<ByteBuffer> getValues() {
			return values;
		}
	}

	/**
	 * Append {@code values} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> rPush(ByteBuffer key, List<ByteBuffer> values) {

		try {
			Assert.notNull(key, "command must not be null!");
			Assert.notNull(values, "Values must not be null!");
		} catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		return rPush(Mono.just(PushCommand.values(values).to(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Append {@link PushCommand#getValues()} to {@link PushCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<PushCommand, Long>> rPush(Publisher<PushCommand> commands);
}
