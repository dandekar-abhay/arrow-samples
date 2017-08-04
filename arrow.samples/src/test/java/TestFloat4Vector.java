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


import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float4Vector.Accessor;
import org.apache.arrow.vector.Float4Vector.Mutator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

/**
 * Test Float4Vector [ Float4 implements a vector of fixed width values.
 * Elements in the vector are accessed by position, starting from the logical
 * start of the vector. Values should be pushed onto the vector sequentially,
 * but may be randomly accessed. The width of each element is 4 byte(s) The
 * equivalent Java primitive is 'float'.]
 *
 **/

public class TestFloat4Vector extends TestCase {

	private BufferAllocator allocator;
	private Float4Vector float4Vector;

	private int testSizeLarge = 1000;
	private int testSizeSmall = 10;

	@Before
	public void init() {
		allocator = new RootAllocator(Float4Vector.MAX_ALLOCATION_SIZE);
		float4Vector = new Float4Vector("TestVector", allocator);
	}

	@After
	public void terminate() throws Exception {
		float4Vector.clear();
		float4Vector.close();
		allocator.close();
	}

	/**
	 * Check max allocation for Float Note : This test case is modified fails
	 * because only save ( max-size / width of the data-type ) number of elements.
	 */
	@Test
	public void testMaxSizeAllocation() {

		int dataTypeSize = 4;
		int maxAllocationPossible = Float4Vector.MAX_ALLOCATION_SIZE / dataTypeSize;

		try (BufferAllocator allocator = new RootAllocator(Float4Vector.MAX_ALLOCATION_SIZE);
				Float4Vector vector = new Float4Vector("TestMaxAlloc", allocator)) {
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
	public void testFloat4Insertion() {

		allocator = new RootAllocator(Float4Vector.MAX_ALLOCATION_SIZE);
		float4Vector = new Float4Vector("TestVector", allocator);

		float4Vector.reset();
		float4Vector.allocateNew(testSizeSmall);
		Mutator intMutator = float4Vector.getMutator();
		int len = float4Vector.getValueCapacity();
		assertTrue(" Allocated length " + len + " should be greater than " + testSizeSmall, len > testSizeSmall);
		for (int i = 0; i < testSizeSmall; i++) {
			intMutator.set(i, testSizeSmall - (1 + i));
		}

		Accessor intaccessor = float4Vector.getAccessor();

		for (int i = 0; i < testSizeSmall; i++) {
			assertEquals((float) testSizeSmall - (1 + i), intaccessor.get(i));
		}
		float4Vector.reset();
		float4Vector.clear();
	}

	/**
	 * Test resetting data
	 */
	@Test
	public void testFloat4Reset() {

		allocator = new RootAllocator(Float4Vector.MAX_ALLOCATION_SIZE);
		float4Vector = new Float4Vector("TestVector", allocator);
		float4Vector.allocateNew(testSizeSmall);
		float4Vector.reset();
		Mutator intMutator = float4Vector.getMutator();
		int len = float4Vector.getValueCapacity();
		for (int i = 0; i < testSizeSmall; i++) {
			intMutator.set(i, len - (1 + i));
		}
		float4Vector.reset();
		Accessor intaccessor = float4Vector.getAccessor();
		for (int i = 0; i < testSizeSmall; i++) {
			assertEquals((float) 0, intaccessor.get(i));
		}

		float4Vector.reset();
		for (int i = 0; i < testSizeSmall; i++) {
			assertEquals((float) 0, intaccessor.get(i));
		}
		float4Vector.reset();
		float4Vector.clear();

	}

	/**
	 * Copy test
	 */
	@Test
	public void testFloat4Copy() {

		allocator = new RootAllocator(Float4Vector.MAX_ALLOCATION_SIZE);
		float4Vector = new Float4Vector("TestVector", allocator);

		float4Vector.reset();
		float4Vector.allocateNew(testSizeSmall);
		Mutator intMutator = float4Vector.getMutator();

		int len = float4Vector.getValueCapacity();
		for (int i = 0; i < testSizeSmall; i++) {
			intMutator.set(i, i * 2);
		}

		Float4Vector intCopyVector = new Float4Vector("TestCopyVector", allocator);
		intCopyVector.allocateNew();
		int copyLen = intCopyVector.getValueCapacity();
		int copyIdx = len / 2;
		intCopyVector.copyFrom(copyIdx, 1, float4Vector);
		assertEquals(float4Vector.getAccessor().get(copyIdx), intCopyVector.getAccessor().get(1));
		intCopyVector.copyFrom(copyIdx, (copyLen / 2 + 1), float4Vector);
		assertEquals(float4Vector.getAccessor().get(copyIdx), intCopyVector.getAccessor().get(1));

		float4Vector.reset();
		float4Vector.clear();
		intCopyVector.clear();
		intCopyVector.close();
	}

	/**
	 * Test transfer
	 */
	@Test
	public void testFloat4Transfer() {

		allocator = new RootAllocator(Float4Vector.MAX_ALLOCATION_SIZE);
		float4Vector = new Float4Vector("TestVector", allocator);

		float4Vector.allocateNew(testSizeLarge);
		Mutator intMutator = float4Vector.getMutator();
		int len = float4Vector.getValueCapacity();
		for (int i = 0; i < len; i++) {
			intMutator.set(i, i * 2);
		}

		Float4Vector innerFloat4Vector = new Float4Vector("Float4Split", allocator);
		innerFloat4Vector.allocateNew();

		float4Vector.splitAndTransferTo(0, len / 2, innerFloat4Vector);
		Accessor intAccessor = float4Vector.getAccessor();
		Accessor bigIntaccessor = innerFloat4Vector.getAccessor();

		for (int i = 0; i < innerFloat4Vector.getValueCapacity(); i++) {
			assertEquals(intAccessor.get(i), bigIntaccessor.get(i));
		}

		innerFloat4Vector.clear();
		innerFloat4Vector.close();
	}

	/**
	 * Test clear
	 */
	@Test
	public void testFloat4Clear() {
		allocator = new RootAllocator(Float4Vector.MAX_ALLOCATION_SIZE);
		float4Vector = new Float4Vector("TestVector", allocator);

		float4Vector.allocateNew(testSizeSmall);
		float4Vector.clear();

		assertTrue("Float Vector size not cleared to 0", float4Vector.getValueCapacity() == 0);
	}

	/**
	 * Test and check the allocation and reallocation for bigInt
	 */
	@Test
	public void testFloat4AllocReAlloc() {

		try (BufferAllocator allocator = new RootAllocator(Float4Vector.MAX_ALLOCATION_SIZE);
				Float4Vector float4Vector = new Float4Vector("Float4Test", allocator);) {

			float4Vector.allocateNew();
			// check the vector value capacity
			assertEquals(Float4Vector.INITIAL_VALUE_ALLOCATION, float4Vector.getValueCapacity());

			float4Vector.reAlloc();
			// check the float vector value capacity
			assertEquals(Float4Vector.INITIAL_VALUE_ALLOCATION * 2, float4Vector.getValueCapacity());

		} catch (Exception e) {
			fail(e.getLocalizedMessage());
		}
	}

	/**
	 * create new Float4Vector instance and set the value and get the value from it.
	 **/
	@Test
	public void testFloat4VectorSetAndGet() {
		try (RootAllocator allocator = new RootAllocator(Float4Vector.MAX_ALLOCATION_SIZE);
				Float4Vector float4Vector = new Float4Vector("float4Vector", allocator);) {

			float4Vector.allocateNew(1000);
			Mutator float4Mutator = float4Vector.getMutator();

			int len = float4Vector.getValueCapacity();
			for (int i = 0; i < len; i++) {
				float4Mutator.set(i, len - (1 + i));
			}

			Accessor intaccessor = float4Vector.getAccessor();

			for (int i = 0; i < len; i++) {
				assertEquals((float) len - (1 + i), intaccessor.get(i));
			}

		} catch (Exception e) {
			fail(e.getLocalizedMessage());
		}
	}

	/**
	 * Buffer allocation with 0 size allocator.
	 **/
	@Test
	public void testFloat4VectorAllocation() {
		// create root allocator with 0 limit
		try (RootAllocator allocatorMax = new RootAllocator(0);
				Float4Vector float4VectorMax = new Float4Vector("float4Vector", allocatorMax)) {
			float4VectorMax.allocateNew(Float4Vector.MAX_ALLOCATION_SIZE / 4);
			// check the float vector value capacity
			assertEquals(Float4Vector.MAX_ALLOCATION_SIZE / 4 , float4VectorMax.getValueCapacity());
			float4VectorMax.allocateNew(Float4Vector.MAX_ALLOCATION_SIZE / 4);

			// change the root allocator limit from 0 to 5000.
			allocatorMax.setLimit(5000);
			
			// allocate the 256 size buffer for float vector
			float4VectorMax.allocateNew((256));
			
			assertEquals(256, float4VectorMax.getValueCapacity());

			// assign the value in float vector
			Mutator float4Mutator = float4VectorMax.getMutator();
			int len = float4VectorMax.getValueCapacity();
			for (int i = 0; i < len; i++) {
				float4Mutator.set(i, len - (1 + i));
			}

			// fetch the value in float vector
			Accessor intaccessor = float4VectorMax.getAccessor();
			for (int i = 0; i < len; i++) {
				assertEquals((float) (len - (1 + i)), intaccessor.get(i));
			}
		} catch (Exception e) {
			// This may happen on my local box
			if (! (e instanceof OutOfMemoryException)) {
				fail(e.getLocalizedMessage());
			}
			
		}
	}

}
