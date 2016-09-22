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

import static org.hamcrest.collection.IsIterableContainingInOrder.*;
import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsEqual.*;
import static org.hamcrest.core.IsNot.*;
import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ReactiveListCommands.PushCommand;
import org.springframework.data.redis.connection.RedisListCommands.Position;

import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 */
public class LettuceReactiveListCommandTests extends LettuceReactiveCommandsTestsBase {

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void rPushShouldAppendValuesCorrectly() {

		nativeCommands.lpush(KEY_1, VALUE_1);

		assertThat(connection.listCommands().rPush(KEY_1_BBUFFER, Arrays.asList(VALUE_2_BBUFFER, VALUE_3_BBUFFER)).block(),
				is(3L));
		assertThat(nativeCommands.lrange(KEY_1, 0, -1), contains(VALUE_1, VALUE_2, VALUE_3));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void lPushShouldPrependValuesCorrectly() {

		nativeCommands.lpush(KEY_1, VALUE_1);

		assertThat(connection.listCommands().lPush(KEY_1_BBUFFER, Arrays.asList(VALUE_2_BBUFFER, VALUE_3_BBUFFER)).block(),
				is(3L));
		assertThat(nativeCommands.lrange(KEY_1, 0, -1), contains(VALUE_3, VALUE_2, VALUE_1));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void rPushXShouldAppendValuesCorrectly() {

		nativeCommands.lpush(KEY_1, VALUE_1);

		assertThat(connection.listCommands().rPushX(KEY_1_BBUFFER, VALUE_2_BBUFFER).block(), is(2L));
		assertThat(nativeCommands.lrange(KEY_1, 0, -1), contains(VALUE_1, VALUE_2));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void lPushXShouldPrependValuesCorrectly() {

		nativeCommands.lpush(KEY_1, VALUE_1);

		assertThat(connection.listCommands().lPushX(KEY_1_BBUFFER, VALUE_2_BBUFFER).block(), is(2L));
		assertThat(nativeCommands.lrange(KEY_1, 0, -1), contains(VALUE_2, VALUE_1));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test(expected = InvalidDataAccessApiUsageException.class)
	public void rPushShouldThrowErrorForMoreThanOneValueWhenUsingExistsOption() {

		connection.listCommands()
				.rPush(
						Mono.just(PushCommand.values(Arrays.asList(VALUE_1_BBUFFER, VALUE_2_BBUFFER)).to(KEY_1_BBUFFER).ifExists()))
				.blockFirst();
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test(expected = InvalidDataAccessApiUsageException.class)
	public void lPushShouldThrowErrorForMoreThanOneValueWhenUsingExistsOption() {

		connection.listCommands()
				.lPush(
						Mono.just(PushCommand.values(Arrays.asList(VALUE_1_BBUFFER, VALUE_2_BBUFFER)).to(KEY_1_BBUFFER).ifExists()))
				.blockFirst();
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void lLenShouldReturnSizeCorrectly() {

		nativeCommands.lpush(KEY_1, VALUE_1, VALUE_2);

		assertThat(connection.listCommands().lLen(KEY_1_BBUFFER).block(), is(2L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void lRangeShouldReturnValuesCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.listCommands().lRange(KEY_1_BBUFFER, 1, 2).block(),
				contains(VALUE_2_BBUFFER, VALUE_3_BBUFFER));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void lTrimShouldReturnValuesCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.listCommands().lTrim(KEY_1_BBUFFER, 1, 2).block(), is(true));
		assertThat(nativeCommands.lrange(KEY_1, 0, -1), not(contains(VALUE_1_BBUFFER)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void lIndexShouldReturnValueCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.listCommands().lIndex(KEY_1_BBUFFER, 1).block(), is(equalTo(VALUE_2_BBUFFER)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void lInsertShouldAddValueCorrectlyBeforeExisting() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2);

		assertThat(
				connection.listCommands().lInsert(KEY_1_BBUFFER, Position.BEFORE, VALUE_2_BBUFFER, VALUE_3_BBUFFER).block(),
				is(3L));
		assertThat(nativeCommands.lrange(KEY_1, 0, -1), contains(VALUE_1, VALUE_3, VALUE_2));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void lInsertShouldAddValueCorrectlyAfterExisting() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2);

		assertThat(
				connection.listCommands().lInsert(KEY_1_BBUFFER, Position.AFTER, VALUE_2_BBUFFER, VALUE_3_BBUFFER).block(),
				is(3L));
		assertThat(nativeCommands.lrange(KEY_1, 0, -1), contains(VALUE_1, VALUE_2, VALUE_3));
	}
}
