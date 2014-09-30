// scalastyle:off
package io.ddf

import org.scalatest.FunSuite
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.scalatest.BeforeAndAfterEach
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfterAll

/**
 * This makes a Logger LOG variable available to the test suite.
 * Also makes beforeEach/afterEach as well as beforeAll/afterAll behaviors available.
 */
@RunWith(classOf[JUnitRunner])
abstract class ATestSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {
	val LOG: Logger = LoggerFactory.getLogger(this.getClass())
}

/**
 * This logs the begin/end of each test with timestamps and test #
 */
abstract class ATimedTestSuite extends ATestSuite {
	private lazy val testNameArray: Array[String] = testNames.toArray
	private var testNumber: Int = 0

	def getCurrentTestName = "Test #%d: %s".format(testNumber + 1, testNameArray(testNumber))

	override def beforeEach = {
		LOG.info("%s started".format(this.getCurrentTestName))
	}

	override def afterEach = {
		testNumber += 1
		LOG.info("%s ended".format(this.getCurrentTestName))
		super.afterEach
	}
}
