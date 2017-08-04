/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntVector.Accessor;
import org.apache.arrow.vector.IntVector.Mutator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestIntVector {
	private BufferAllocator allocator;
	private IntVector intVector;

	private int testSizeLarge = 1000;
	private int testSizeSmall = 10;

	@Before
	public void init() {
		allocator = new RootAllocator(Integer.MAX_VALUE);
		intVector = new IntVector("IntTest", allocator);
	}

	@After
	public void terminate() throws Exception {
		intVector.clear();
		intVector.close();
		allocator.close();
	}

	/*
	 * Test inserting data
	 */
	@Test
	public void testIntInsertion() {

		Mutator intMutator = intVector.getMutator();
		int len = intVector.getValueCapacity();
		for (int i = 0; i < len; i++) {
			intMutator.set(i, len - (1 + i));
		}

		Accessor intaccessor = intVector.getAccessor();

		for (int i = 0; i < len; i++) {
			assertEquals(len - (1 + i), intaccessor.get(i));
		}
		intVector.reset();
		intVector.clear();
	}

	/*
	 * Test reset data
	 */
	@Test
	public void testIntReset() {

		intVector.allocateNew();
		Mutator intMutator = intVector.getMutator();
		int len = intVector.getValueCapacity();
		for (int i = 0; i < len; i++) {
			intMutator.set(i, len - (1 + i));
		}

		intVector.reset();
		Accessor intaccessor = intVector.getAccessor();
		for (int i = 0; i < len; i++) {
			assertEquals(0, intaccessor.get(i));
			;
		}
		intVector.reset();
		intVector.clear();
	}

	/**
	 * Copy test
	 */
	@Test
	public void testIntCopy() {
		intVector.reset();
		intVector.allocateNew(testSizeSmall);
		Mutator intMutator = intVector.getMutator();

		int len = intVector.getValueCapacity();
		for (int i = 0; i < testSizeSmall; i++) {
			intMutator.set(i, i * 2);
		}

		IntVector intCopyVector = new IntVector("BigIntTestCopy", allocator);
		intCopyVector.allocateNew();
		int copyLen = intCopyVector.getValueCapacity();
		int copyIdx = len / 2;
		intCopyVector.copyFrom(copyIdx, 1, intVector);
		assertEquals(intVector.getAccessor().get(copyIdx), intCopyVector.getAccessor().get(1));
		intCopyVector.copyFrom(copyIdx, (copyLen / 2 + 1), intVector);
		assertEquals(intVector.getAccessor().get(copyIdx), intCopyVector.getAccessor().get(1));
		

		intVector.reset();
		intVector.clear();
		intCopyVector.clear();
		intCopyVector.close();
	}

	
	/**
	 * Test transfer
	 */
	@Test
	public void testIntTransfer() {

		intVector.allocateNew(testSizeLarge);
		Mutator intMutator = intVector.getMutator();
		int len = intVector.getValueCapacity();
		for (int i = 0; i < len; i++) {
			intMutator.set(i, i * 2);
		}

		IntVector innerIntVector = new IntVector("IntTestSplit", allocator);
		innerIntVector.allocateNew();

		intVector.splitAndTransferTo(0, len / 2, innerIntVector);
		Accessor intAccessor = intVector.getAccessor();
		Accessor bigIntaccessor = innerIntVector.getAccessor();

		for (int i = 0; i < innerIntVector.getValueCapacity(); i++) {
			assertEquals(intAccessor.get(i), bigIntaccessor.get(i));
		}

		innerIntVector.clear();
		innerIntVector.close();
	}

	/**
	 * Test clear
	 */
	@Test
	public void testIntClear() {

		intVector.allocateNew(testSizeSmall);
		intVector.clear();

		assertTrue("BigInt Vector size not cleared to 0", intVector.getValueCapacity() == 0);
	}

}
