package ai.lum.odinson.lucene

import java.util.{ Map => JMap, Set => JSet }
import org.apache.lucene.index._
import org.apache.lucene.search._
import org.apache.lucene.search.spans._

/** Wraps a SpanQuery to add OdinQuery functionality. */
class OdinQueryWrapper(val query: SpanQuery) extends OdinQuery {

  override def hashCode: Int = mkHash(query)

  def getField(): String = query.getField()

  def toString(field: String): String = s"Wrapped(${query.toString(field)})"

  override def createWeight(searcher: IndexSearcher, needsScores: Boolean): OdinWeight = {
    val weight = query.createWeight(searcher, needsScores).asInstanceOf[SpanWeight]
    val termContexts = SpanQuery.getTermContexts(weight)
    new OdinWeightWrapper(this, searcher, termContexts, weight)
  }

  override def rewrite(reader: IndexReader): Query = {
    val rewritten = query.rewrite(reader).asInstanceOf[SpanQuery]
    if (query != rewritten) {
      new OdinQueryWrapper(rewritten)
    } else {
      super.rewrite(reader)
    }
  }

}

class OdinWeightWrapper(
    query: OdinQuery,
    searcher: IndexSearcher,
    termContexts: JMap[Term, TermContext],
    val weight: SpanWeight
) extends OdinWeight(query, searcher, termContexts) {

  def extractTerms(terms: JSet[Term]): Unit = weight.extractTerms(terms)

  def extractTermContexts(contexts: JMap[Term, TermContext]): Unit = {
    weight.extractTermContexts(contexts)
  }

  def getSpans(context: LeafReaderContext, requiredPostings: SpanWeight.Postings): OdinSpans = {
    val spans = weight.getSpans(context, requiredPostings)
    if (spans == null) null else new OdinSpansWrapper(spans)
  }

}

class OdinSpansWrapper(val spans: Spans) extends OdinSpans {
  def nextDoc(): Int = spans.nextDoc()
  def advance(target: Int): Int = spans.advance(target)
  def docID(): Int = spans.docID()
  def nextStartPosition(): Int = spans.nextStartPosition()
  def startPosition(): Int = spans.startPosition()
  def endPosition(): Int = spans.endPosition()
  def cost(): Long = spans.cost()
  def collect(collector: SpanCollector): Unit = spans.collect(collector)
  def positionsCost(): Float = spans.positionsCost()
  override def asTwoPhaseIterator(): TwoPhaseIterator = spans.asTwoPhaseIterator()
  override def width(): Int = spans.width()
}