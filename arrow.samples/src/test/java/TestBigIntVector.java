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
import static org.junit.Assert.fail;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BigIntVector.Accessor;
import org.apache.arrow.vector.BigIntVector.Mutator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestBigIntVector {
	private BufferAllocator allocator;
	private BigIntVector bigIntVector;

	private int testSizeLarge = 1000;
	private int testSizeSmall = 10;

	@Before
	public void init() {
		allocator = new RootAllocator(Integer.MAX_VALUE);
		bigIntVector = new BigIntVector("BigIntTest", allocator);
	}

	@After
	public void terminate() throws Exception {
		bigIntVector.clear();
		bigIntVector.close();
		allocator.close();
	}

	/**
	 * Check max allocation for BigInt Note : This test case fails
	 */
	@Test
	public void testMaxSizeAllocation() {

		int dataTypeSize = 8;
		int maxAllocationPossible = BigIntVector.MAX_ALLOCATION_SIZE / dataTypeSize;

		try (BufferAllocator allocator = new RootAllocator(BigIntVector.MAX_ALLOCATION_SIZE);
				BigIntVector vector = new BigIntVector("TestMaxAlloc", allocator)) {
			vector.reset();
			vector.allocateNew(maxAllocationPossible);
			int len = vector.getValueCapacity();
			assertTrue("Allocated size " + len + " is not equal to requested size" + maxAllocationPossible,
					len == maxAllocationPossible);
		}
	}

	/**
	 * Test inserting data
	 */
	@Test
	public void testBigIntInsertion() {
		bigIntVector.reset();
		bigIntVector.allocateNew(testSizeSmall);
		Mutator intMutator = bigIntVector.getMutator();
		int len = bigIntVector.getValueCapacity();
		assertTrue(" Allocated length " + len + " should be greater than " + testSizeSmall, len > testSizeSmall);
		for (int i = 0; i < testSizeSmall; i++) {
			intMutator.set(i, testSizeSmall - (1 + i));
		}

		Accessor intaccessor = bigIntVector.getAccessor();

		for (int i = 0; i < testSizeSmall; i++) {
			assertEquals(testSizeSmall - (1 + i), intaccessor.get(i));
		}
		bigIntVector.reset();
		bigIntVector.clear();
	}

	/**
	 * Test resetting data
	 */
	@Test
	public void testBigIntReset() {
		bigIntVector.allocateNew(testSizeSmall);
		bigIntVector.reset();
		Mutator intMutator = bigIntVector.getMutator();
		int len = bigIntVector.getValueCapacity();
		for (int i = 0; i < testSizeSmall; i++) {
			intMutator.set(i, len - (1 + i));
		}
		bigIntVector.reset();
		Accessor intaccessor = bigIntVector.getAccessor();
		for (int i = 0; i < testSizeSmall; i++) {
			assertEquals(0, intaccessor.get(i));
		}

		bigIntVector.reset();
		for (int i = 0; i < testSizeSmall; i++) {
			assertEquals(0, intaccessor.get(i));
		}
		bigIntVector.reset();
		bigIntVector.clear();

	}

	/**
	 * Copy test
	 */
	@Test
	public void testBigIntCopy() {
		bigIntVector.reset();
		bigIntVector.allocateNew(testSizeSmall);
		Mutator intMutator = bigIntVector.getMutator();

		int len = bigIntVector.getValueCapacity();
		for (int i = 0; i < testSizeSmall; i++) {
			intMutator.set(i, i * 2);
		}

		BigIntVector intCopyVector = new BigIntVector("BigIntTestCopy", allocator);
		intCopyVector.allocateNew();
		int copyLen = intCopyVector.getValueCapacity();
		int copyIdx = len / 2;
		intCopyVector.copyFrom(copyIdx, 1, bigIntVector);
		assertEquals(bigIntVector.getAccessor().get(copyIdx), intCopyVector.getAccessor().get(1));
		intCopyVector.copyFrom(copyIdx, (copyLen / 2 + 1), bigIntVector);
		assertEquals(bigIntVector.getAccessor().get(copyIdx), intCopyVector.getAccessor().get(1));

		bigIntVector.reset();
		bigIntVector.clear();
		intCopyVector.clear();
		intCopyVector.close();
	}

	/**
	 * Test transfer
	 */
	@Test
	public void testBigIntTransfer() {

		bigIntVector.allocateNew(testSizeLarge);
		Mutator intMutator = bigIntVector.getMutator();
		int len = bigIntVector.getValueCapacity();
		for (int i = 0; i < len; i++) {
			intMutator.set(i, i * 2);
		}

		BigIntVector innerBigIntVector = new BigIntVector("IntTestSpllit", allocator);
		innerBigIntVector.allocateNew();

		bigIntVector.splitAndTransferTo(0, len / 2, innerBigIntVector);
		Accessor intAccessor = bigIntVector.getAccessor();
		Accessor bigIntaccessor = innerBigIntVector.getAccessor();

		for (int i = 0; i < innerBigIntVector.getValueCapacity(); i++) {
			assertEquals(intAccessor.get(i), bigIntaccessor.get(i));
		}

		innerBigIntVector.clear();
		innerBigIntVector.close();
	}

	/**
	 * Test clear
	 */
	@Test
	public void testBigIntClear() {

		bigIntVector.allocateNew(testSizeSmall);
		bigIntVector.clear();

		assertTrue("BigInt Vector size not cleared to 0", bigIntVector.getValueCapacity() == 0);
	}

	/**
	 * Test and check the allocation and reallocation for bigInt
	 */
	@Test
	public void testBigIntAllocReAlloc() {

		try (BufferAllocator allocator = new RootAllocator(BigIntVector.MAX_ALLOCATION_SIZE);
				BigIntVector bigIntVector = new BigIntVector("BigIntTest", allocator);) {

			bigIntVector.allocateNew();
			// check the vector value capacity
			assertEquals(BigIntVector.INITIAL_VALUE_ALLOCATION, bigIntVector.getValueCapacity());

			bigIntVector.reAlloc();
			// check the float vector value capacity
			assertEquals(BigIntVector.INITIAL_VALUE_ALLOCATION * 2, bigIntVector.getValueCapacity());

		} catch (Exception e) {
			fail(e.getLocalizedMessage());
		}
	}

}
