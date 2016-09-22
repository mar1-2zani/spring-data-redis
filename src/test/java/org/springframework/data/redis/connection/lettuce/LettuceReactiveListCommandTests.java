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
import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

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
}
