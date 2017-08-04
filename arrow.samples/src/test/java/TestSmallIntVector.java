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
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.SmallIntVector.Accessor;
import org.apache.arrow.vector.SmallIntVector.Mutator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSmallIntVector {

	private BufferAllocator allocator;
	private SmallIntVector smallIntVector;

	private int testSizeLarge = 1000;
	private int testSizeSmall = 10;

	@Before
	public void init() {
		allocator = new RootAllocator(Integer.MAX_VALUE);
		smallIntVector = new SmallIntVector("IntTest", allocator);
	}

	@After
	public void terminate() throws Exception {
		smallIntVector.clear();
		smallIntVector.close();
		allocator.close();
	}

	/**
	 * Check max allocation for BigInt Note : This test case fails because for a
	 * particular datatype, we can only save ( max-size / width of the data-type )
	 * number of elements.
	 */
	@Test
	public void testMaxSizeAllocation() {

		int dataTypeSize = 2;
		int maxAllocationPossible = SmallIntVector.MAX_ALLOCATION_SIZE / dataTypeSize;

		try (BufferAllocator allocator = new RootAllocator(SmallIntVector.MAX_ALLOCATION_SIZE);
				SmallIntVector smallIntVector = new SmallIntVector("SmallIntTest", allocator)) {
			smallIntVector.reset();
			smallIntVector.allocateNew(maxAllocationPossible);
			int len = smallIntVector.getValueCapacity();
			assertTrue("Allocated size " + len + " is not equal to requested size" + maxAllocationPossible,
					len == maxAllocationPossible);
		}
	}

	/**
	 * create new Float8Vector instance and set the value and get the value from it.
	 **/
	@Test
	public void testSmallIntInsertion() {

		try (BufferAllocator allocator = new RootAllocator(SmallIntVector.MAX_ALLOCATION_SIZE);
				SmallIntVector smallIntVector = new SmallIntVector("SmallIntTest", allocator)) {

			Mutator smallIntMutator = smallIntVector.getMutator();
			int len = smallIntVector.getValueCapacity();
			for (int i = 0; i < len; i++) {
				smallIntMutator.set(i, len - (1 + i));
			}

			Accessor intaccessor = smallIntVector.getAccessor();

			for (int i = 0; i < len; i++) {
				assertEquals(len - (1 + i), intaccessor.get(i));
				;
			}

		}
	}

	/**
	 * set the value into vector and reset it.
	 **/
	@Test
	public void testSmallIntReset() {

		try (BufferAllocator allocator = new RootAllocator(SmallIntVector.MAX_ALLOCATION_SIZE);
				SmallIntVector smallIntVector = new SmallIntVector("SmallIntTest", allocator)) {

			smallIntVector.allocateNew();
			Mutator intMutator = smallIntVector.getMutator();
			int len = smallIntVector.getValueCapacity();
			for (int i = 0; i < len; i++) {
				intMutator.set(i, len - (1 + i));
			}

			smallIntVector.reset();
			Accessor intaccessor = smallIntVector.getAccessor();
			for (int i = 0; i < len; i++) {
				assertEquals(0, intaccessor.get(i));
				;
			}
		}

	}

	/**
	 * Copy test
	 */
	@Test
	public void testSmallIntCopy() {
		smallIntVector.reset();
		smallIntVector.allocateNew(testSizeSmall);
		Mutator intMutator = smallIntVector.getMutator();

		int len = smallIntVector.getValueCapacity();
		for (int i = 0; i < testSizeSmall; i++) {
			intMutator.set(i, i * 2);
		}

		SmallIntVector intCopyVector = new SmallIntVector("BigIntTestCopy", allocator);
		intCopyVector.allocateNew();
		int copyLen = intCopyVector.getValueCapacity();
		int copyIdx = len / 2;
		intCopyVector.copyFrom(copyIdx, 1, smallIntVector);
		assertEquals(smallIntVector.getAccessor().get(copyIdx), intCopyVector.getAccessor().get(1));
		intCopyVector.copyFrom(copyIdx, (copyLen / 2 + 1), smallIntVector);
		assertEquals(smallIntVector.getAccessor().get(copyIdx), intCopyVector.getAccessor().get(1));

		smallIntVector.reset();
		smallIntVector.clear();
		intCopyVector.clear();
		intCopyVector.close();
	}

	/**
	 * Test transfer
	 */
	@Test
	public void testSmallIntTransfer() {

		smallIntVector.allocateNew(testSizeLarge);
		Mutator intMutator = smallIntVector.getMutator();
		int len = smallIntVector.getValueCapacity();
		for (int i = 0; i < len; i++) {
			intMutator.set(i, i * 2);
		}

		SmallIntVector innerIntVector = new SmallIntVector("IntTestSplit", allocator);
		innerIntVector.allocateNew();

		smallIntVector.splitAndTransferTo(0, len / 2, innerIntVector);
		Accessor intAccessor = smallIntVector.getAccessor();
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
	public void testSmallIntClear() {

		smallIntVector.allocateNew(testSizeSmall);
		smallIntVector.clear();

		assertTrue("BigInt Vector size not cleared to 0", smallIntVector.getValueCapacity() == 0);
	}

	/**
	 * Buffer allocation and reallocation .
	 **/
	@Test
	public void testSmallIntAllocReAlloc() {

		try (BufferAllocator allocator = new RootAllocator(SmallIntVector.MAX_ALLOCATION_SIZE);
				SmallIntVector smallIntVector = new SmallIntVector("SmallIntTest", allocator);) {

			smallIntVector.allocateNew();
			// check the vector value capacity
			assertEquals(SmallIntVector.INITIAL_VALUE_ALLOCATION, smallIntVector.getValueCapacity());

			smallIntVector.reAlloc();
			// check the float vector value capacity
			assertEquals(Float8Vector.INITIAL_VALUE_ALLOCATION * 2, smallIntVector.getValueCapacity());

		} catch (Exception e) {
			fail(e.getLocalizedMessage());
		}
	}

}
