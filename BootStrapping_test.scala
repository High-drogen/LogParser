class Bootstrap(numSamples: Int = 1000, s: Vector[Double] => Double, inputDistribution: Vector[Double], randomizer: Random) {

  lazy val bootstrappedReplicates: Array[Double] =
    ( 0 until numSamples )./:( new mutable.ArrayBuffer[Double] )( ( buf, _ ) => buf += createBootstrapSample ).toArray

  def createBootstrapSample: Double {}

  lazy val mean = bootstrappedReplicates.reduce( _ + _ )/numSamples

  def error: Double = {}
}

def createBootstrapSample: Double = s(
      ( 0 until inputDistribution.size )./:( new mutable.ArrayBuffer[Double] )(
        ( buf, _ ) => {
          randomizer.setSeed( randomizer.nextLong )
          val randomValueIndex = randomizer.nextInt( inputDistribution.size )
          buf += inputDistribution( randomValueIndex )
        }
      ).toVector )
	
def error: Double = {
	val sumOfSquaredDiff = bootstrappedReplicates.reduce(
		(s1: Double, s2: Double) =>
			(s1 - mean) (s1 - mean) +  (s2 - mean)*(s2 - mean)
	    )
		
		Math.sqrt(sumOfSquaredDiff / (numSamples - 1))
}
	   
def bootstrapEvaluation(dist: RealDistribution, random: Random, gen: (Double, Double)): (Double, Double) = {

		val inputDistribution = (0 until 5000)./:(new ArrayBuffer[(Double, Double)])
		(
			( buf, _ ) => {
				val x = gen._1 * random.nextDouble - gen._2
				buf += ( ( x, dist.density( x ) ) )
			}
		).toVector

		val mean = (x: Vector[Double]) => x.sum/x.length
		val bootstrap = new Bootstrap(
			numReplicates,
			mean, 
			inputDistribution.map( _._2 ), 
			new Random( System.currentTimeMillis)
		)

		val meanS = bootstrap.bootstrappedReplicates.sum / 
					bootstrap.bootstrappedReplicates.size
		val sProb = bootstrap.bootstrappedReplicates.map(_ - meanS)
		// .. plotting histogram of distribution sProb
	    (bootstrap.mean, bootstrap.error)
}