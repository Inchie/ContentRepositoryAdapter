<?php
namespace Flowpack\ElasticSearch\ContentRepositoryAdaptor\Eel;


/*                                                                                                  *
 * This script belongs to the TYPO3 Flow package "Flowpack.ElasticSearch.ContentRepositoryAdaptor". *
 *                                                                                                  *
 * It is free software; you can redistribute it and/or modify it under                              *
 * the terms of the GNU Lesser General Public License, either version 3                             *
 *  of the License, or (at your option) any later version.                                          *
 * 
 *                                                                                                  *
 * The TYPO3 project - inspiring people to share!                                                   *
 *                                                                                                  */

use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception\QueryBuildingException;
use TYPO3\Eel\ProtectedContextAwareInterface;
use TYPO3\Flow\Annotations as Flow;
use TYPO3\TYPO3CR\Domain\Model\NodeInterface;
use TYPO3\TYPO3CR\Search\Search\QueryBuilderInterface;

/**
 * Query Builder for ElasticSearch Queries
 */
class ElasticSearchQueryBuilder implements QueryBuilderInterface, ProtectedContextAwareInterface  {

	/**
	 * @var int
	 */
	protected $hits = 0;

	/**
	 * @Flow\Inject
	 * @var \Flowpack\ElasticSearch\ContentRepositoryAdaptor\ElasticSearchClient
	 */
	protected $elasticSearchClient;

	/**
	 * The node inside which searching should happen
	 *
	 * @var NodeInterface
	 */
	protected $contextNode;

	/**
	 * @Flow\Inject
	 * @var \Flowpack\ElasticSearch\ContentRepositoryAdaptor\LoggerInterface
	 */
	protected $logger;

	/**
	 * @var boolean
	 */
	protected $logThisQuery = FALSE;

	/**
	 * @var string
	 */
	protected $logMessage;

	/**
	 * The ElasticSearch request, as it is being built up.
	 * @var array
	 */
	protected $request = array(
		'size' => 0,
		'aggs' => array(
			'filtered' => array(
				'filter' => array(
					'bool' => array(
						'must' => array(),
						'should' => array(),
						'must_not' => array(
							// Filter out all hidden elements
							array(
								'term' => array('_hidden' => TRUE)
							),
							// if now < hiddenBeforeDateTime: HIDE
							// -> hiddenBeforeDateTime > now
							array(
								'range' => array('_hiddenBeforeDateTime' => array(
									'gt' => 'now'
								))
							),
							array(
								'range' => array('_hiddenAfterDateTime' => array(
									'lt' => 'now'
								))
							),
						),
					)
				),
				'aggs' => array(
					'top_node_hits' => array(
						'top_hits' => array(
							'sort' => array(),
							'_source' => array(
								"include" => array(
									'__path'
								)
							),
							'size' => 1
						)
					)
				)
			)
		)
	);

	/**
	 * HIGH-LEVEL API
	 */

	/**
	 * Filter by node type, taking inheritance into account.
	 *
	 * @param string $nodeType the node type to filter for
	 * @return ElasticSearchQueryBuilder
	 */
	public function nodeType($nodeType) {
		// on indexing, __typeAndSupertypes contains the typename itself and all supertypes, so that's why we can
		// use a simple term filter here.

		// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-term-filter.html
		return $this->queryFilter('term', array('__typeAndSupertypes' => $nodeType));
	}

	/**
	 * Sort descending by $propertyName
	 *
	 * @param string $propertyName the property name to sort by
	 * @return ElasticSearchQueryBuilder
	 */
	public function sortDesc($propertyName) {
		// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-request-sort.html
		$this->appendAtPath('aggs.filtered.aggs.top_node_hits.top_hits.sort', array($propertyName => array('order' => 'desc')));
		return $this;
	}

	/**
	 * Sort ascending by $propertyName
	 *
	 * @param string $propertyName the property name to sort by
	 * @return ElasticSearchQueryBuilder
	 */
	public function sortAsc($propertyName) {
		$this->appendAtPath('aggs.filtered.aggs.top_node_hits.top_hits.sort', array($propertyName => array('order' => 'asc')));
		return $this;
	}

	/**
	 * add an exact-match query for a given property
	 *
	 * @param string $propertyName Name of the property
	 * @param mixed $value Value for comparison
	 * @return ElasticSearchQueryBuilder
	 */
	public function exactMatch($propertyName, $value) {
		if ($value instanceof NodeInterface) {
			$value = $value->getIdentifier();
		}
		return $this->queryFilter('term', array($propertyName => $value));
	}

	/**
	 * add a range filter (gt) for the given property
	 *
	 * @param string $propertyName Name of the property
	 * @param mixed $value Value for comparison
	 * @return ElasticSearchQueryBuilder
	 */
	public function greaterThan($propertyName, $value) {
		return $this->queryFilter('range', array($propertyName => array('gt' => $value)));
	}

	/**
	 * add a range filter (gte) for the given property
	 *
	 * @param string $propertyName Name of the property
	 * @param mixed $value Value for comparison
	 * @return ElasticSearchQueryBuilder
	 */
	public function greaterThanOrEqual($propertyName, $value) {
		return $this->queryFilter('range', array($propertyName => array('gte' => $value)));
	}
	/**
	 * add a range filter (lt) for the given property
	 *
	 * @param string $propertyName Name of the property
	 * @param mixed $value Value for comparison
	 * @return ElasticSearchQueryBuilder
	 */
	public function lessThan($propertyName, $value) {
		return $this->queryFilter('range', array($propertyName => array('lt' => $value)));
	}

	/**
	 * add a range filter (lte) for the given property
	 *
	 * @param string $propertyName Name of the property
	 * @param mixed $value Value for comparison
	 * @return ElasticSearchQueryBuilder
	 */
	public function lessThanOrEqual($propertyName, $value) {
		return $this->queryFilter('range', array($propertyName => array('lte' => $value)));
	}

	/**
	 * will return the average over the filtered items
	 *
	 * @param string $propertyName
	 * @return $this
	 * @throws QueryBuildingException
	 */
	public function average($propertyName) {
		return $this->queryStat('avg', $propertyName);
	}

	/**
	 * will return the minimum value over the filtered items
	 *
	 * @param string $propertyName
	 * @return $this
	 */
	public function minimum($propertyName) {
		return $this->queryStat('min', $propertyName);
	}

	/**
	 * will return the maximum value over the filtered items
	 *
	 * @param string $propertyName
	 * @return $this
	 */
	public function maximum($propertyName) {
		return $this->queryStat('max', $propertyName);
	}

	/**
	 * will return the sum value over the filtered items
	 *
	 * @param $propertyName
	 * @return ElasticSearchQueryBuilder
	 * @throws QueryBuildingException
	 */
	public function sum($propertyName) {
		return $this->queryStat('sum', $propertyName);
	}

	/**
	 * not supported for aggregations. The documents are automatically counted.
	 * @return int|void
	 */
	public function count() {}

	/**
	 * will return the following stats over the filtered items
	 * avg, min, max, count, sum
	 *
	 * @param $propertyName
	 * @return ElasticSearchQueryBuilder
	 * @throws QueryBuildingException
	 */
	public function stats($propertyName) {
		return $this->queryStat('stats', $propertyName);
	}

	/**
	 * will return the following extended stats over the filtered items
	 * sum_of_squares, variance, deviation
	 *
	 * @param $propertyName
	 * @return ElasticSearchQueryBuilder
	 * @throws QueryBuildingException
	 */
	public function extendedStats($propertyName) {
		return $this->queryStat('extended_stats', $propertyName);
	}

	/**
	 * will return buckets
	 *
	 * @param $propertyName
	 * @return $this
	 */
	public function groupByProperty($propertyName) {
		$this->request['aggs']['filtered']['aggs']['group_by_state'] = array('terms' => array('field' => $propertyName));
		return $this;
	}

	/**
	 * LOW-LEVEL API
	 */

	/**
	 *  Add a statistic to query.aggs.aggs
	 * @param string $calcAggregator
	 * @param string $propertyName
	 * @throws \Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception\QueryBuildingException
	 * @return ElasticSearchQueryBuilder
	 */
	public function queryStat($calcAggregator, $propertyName) {
		if(!in_array($calcAggregator, array('avg', 'min', 'max', 'sum', 'stats', 'extended_stats'))) {
			throw new QueryBuildingException('The given calc aggregator "' . $calcAggregator . '" is not supported. Must be one of "avg", "min", "max", "sum", "stats", "extended_stats".', 1383716083);
		}

		$fieldName = $calcAggregator . '_property';
		if(!array_key_exists($fieldName, $this->request['aggs']['filtered']['aggs'])) {
			$this->request['aggs']['filtered']['aggs'][$fieldName] = array();
		}
		$this->request['aggs']['filtered']['aggs'][$fieldName] = array($calcAggregator => array('field' => $propertyName));
		return $this;
	}

	/**
	 * Add a filter to query.filtered.filter
	 *
	 * @param string $filterType
	 * @param mixed $filterOptions
	 * @param string $clauseType one of must, should, must_not
	 * @throws \Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception\QueryBuildingException
	 * @return ElasticSearchQueryBuilder
	 */
	public function queryFilter($filterType, $filterOptions, $clauseType = 'must') {
		if (!in_array($clauseType, array('must', 'should', 'must_not'))) {
			throw new QueryBuildingException('The given clause type "' . $clauseType . '" is not supported. Must be one of "mmust", "should", "must_not".', 1383716082);
		}
		return $this->appendAtPath('aggs.filtered.filter.bool.' . $clauseType, array($filterType => $filterOptions));
	}

	/**
	 * Append $data to the given array at $path inside $this->request.
	 *
	 * Low-level method to manipulate the ElasticSearch Query
	 *
	 * @param string $path
	 * @param array $data
	 * @throws \Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception\QueryBuildingException
	 * @return ElasticSearchQueryBuilder
	 */
	public function appendAtPath($path, array $data) {
		$currentElement =& $this->request;
		foreach (explode('.', $path) as $pathPart) {
			if (!isset($currentElement[$pathPart])) {
				throw new QueryBuildingException('The element at path "' . $path . '" was not an array (failed at "' . $pathPart . '").', 1383716367);
			}
			$currentElement =& $currentElement[$pathPart];
		}
		$currentElement[] = $data;
		return $this;
	}

	/**
	 * @param int $limit
	 * @return $this
	 */
	public function nodesByLimit($limit = 1) {
		$this->request['aggs']['filtered']['aggs']['top_node_hits']['top_hits']['size'] = $limit;
		$this->hits = 1;
		return $this;
	}

	/**
	 * Get the ElasticSearch request as we need it
	 *
	 * @return array
	 */
	public function getRequest() {
		return $this->request;
	}

	/**
	 * Log the current request to the ElasticSearch log for debugging after it has been executed.
	 *
	 * @param string $message an optional message to identify the log entry
	 * @return $this
	 */
	public function log($message = NULL) {
		$this->logThisQuery = TRUE;
		$this->logMessage = $message;
		return $this;
	}

	/**
	 * Execute the query and return the list of nodes as result
	 *
	 * @return mixed
	 */
	public function execute() {
		$timeBefore = microtime(TRUE);
		$response = $this->elasticSearchClient->getIndex()->request('GET', '/_search', array(), json_encode($this->request));
		$timeAfterwards = microtime(TRUE);

		if ($this->logThisQuery === TRUE) {
			$this->logger->log('Query Log (' . $this->logMessage . '): ' . json_encode($this->request) . ' -- execution time: ' . (($timeAfterwards-$timeBefore)*1000) . ' ms -- Limit: ' .  ' -- Number of results returned: ' .  ' -- Total Results: ', LOG_DEBUG);
		}

		$treatedContent = $response->getTreatedContent();

		if($this->hits == 1) {
			$hits = $treatedContent['aggregations']['filtered']['top_node_hits']['hits']['hits'];
			$nodes = $this->buildNodesByHits($hits);
			return array_values($nodes);
		} else {
			return $treatedContent['aggregations']['filtered'];
		}
	}

	/**
	 * @param $hits
	 * @return array
	 */
	protected function buildNodesByHits($hits) {
		$nodes = array();
		foreach ($hits as $hit) {
			// with ElasticSearch 1.0 fields always returns an array,
			// see https://github.com/Flowpack/Flowpack.ElasticSearch.ContentRepositoryAdaptor/issues/17
			$nodePath = $hit['_source']['__path'];
			$node = $this->contextNode->getNode($nodePath);
			if ($node instanceof NodeInterface) {
				$nodes[$node->getIdentifier()] = $node;
			}
		}
		return $nodes;
	}

	/**
	 * Sets the starting point for this query. Search result should only contain nodes that
	 * match the context of the given node and have it as parent node in their rootline.
	 *
	 * @param NodeInterface $contextNode
	 * @return QueryBuilderInterface
	 */
	public function query(NodeInterface $contextNode) {
		// on indexing, the __parentPath is tokenized to contain ALL parent path parts,
		// e.g. /foo, /foo/bar/, /foo/bar/baz; to speed up matching.. That's why we use a simple "term" filter here.
		// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-term-filter.html
		$this->queryFilter('term', array('__parentPath' => $contextNode->getPath()));
		//
		// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-terms-filter.html
		$this->queryFilter('terms', array('__workspace' => array('live', $contextNode->getContext()->getWorkspace()->getName())));
		$this->contextNode = $contextNode;
		return $this;
	}

	/**
	 * All methods are considered safe
	 *
	 * @param string $methodName
	 * @return boolean
	 */
	public function allowsCallOfMethod($methodName) {
		return TRUE;
	}

	public function from($from){}

	/**
	 * Match the searchword against the fulltext index
	 *
	 * @param string $searchWord
	 * @return QueryBuilderInterface
	 */
	public function fulltext($searchWord){
		$this->appendAtPath('aggs.filtered.filter.bool.must', array(
			'query_string' => array(
				'query' => $searchWord
			)
		));
		return $this;
	}

	public function limit($limit){}
}