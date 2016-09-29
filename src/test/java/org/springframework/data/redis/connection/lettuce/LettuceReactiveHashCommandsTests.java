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

import static org.hamcrest.core.Is.*;
import static org.junit.Assert.*;

import org.junit.Test;

/**
 * @author Christoph Strobl
 */
public class LettuceReactiveHashCommandsTests extends LettuceReactiveCommandsTestsBase {

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void hSetShouldOperateCorrectly() {
		assertThat(connection.hashCommands().hSet(KEY_1_BBUFFER, KEY_2_BBUFFER, VALUE_1_BBUFFER).block(), is(true));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void hSetNxShouldOperateCorrectly() {
		assertThat(connection.hashCommands().hSetNX(KEY_1_BBUFFER, KEY_2_BBUFFER, VALUE_1_BBUFFER).block(), is(true));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void hSetNxShouldReturnFalseIfFieldAlreadyExists() {

		nativeCommands.hset(KEY_1, KEY_2, VALUE_1);

		assertThat(connection.hashCommands().hSetNX(KEY_1_BBUFFER, KEY_2_BBUFFER, VALUE_1_BBUFFER).block(), is(false));
	}

}
