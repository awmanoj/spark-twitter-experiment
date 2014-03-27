import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming._

object TwitterExperiment {
  def main(args: Array[String]) = {
    val checkPointDir = sys.env.getOrElse("CHECKPOINT_DIR", "/tmp")
    val outputDir = sys.env.getOrElse("OUTPUT_DIR","/tmp/tweets")

    TwitterUtils.configureTwitterCredentials()

    // get spark streaming context
    val ssc = new StreamingContext("local[4]", "Twitter Experiment", Seconds(1))

    // new Twitter Stream
    val tweets = ssc.twitterStream()

    // transform the statuses dstream to get hashTags dstream
    val statuses = tweets.map(status => "@" + status.getUser().getScreenName() + ": " + status.getText())
        statuses.print()

    ssc.checkpoint(checkPointDir)

    // start
    ssc.start()
    //ssc.awaitTermination()
  }
}
