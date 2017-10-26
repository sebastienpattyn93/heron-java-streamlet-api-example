JAR := target/heron-java-streamlet-api-example-latest-jar-with-dependencies.jar
ROOT_LIB=io.streaml.heron.streamlet

assembly:
	mvn assembly:assembly

wire-requests: assembly
	heron submit local ${JAR} ${ROOT_LIB}.WireRequestsTopology WireRequests

wire-requests-kill:
	heron kill local WireRequests

impressions-and-clicks: assembly
	heron submit local ${JAR} ${ROOT_LIB}.ImpressionsAndClicksTopology ImpressionsAndClicks

impressions-and-clicks-kill:
	heron kill local ImpressionsAndClicks

windowed-word-count: assembly
	heron submit local ${JAR} ${ROOT_LIB}.WindowedWordCount WindowedWordCount

windowed-word-count-kill:
	heron kill local WindowedWordCount
