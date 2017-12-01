<?php

namespace ElasticsearchMigrator;

use Elasticsearch\Client as Client;
use ElasticsearchMigrator\Exceptions\IndexAlreadyExistsException;
use ElasticsearchMigrator\Exceptions\IndexNotFoundException;
use ElasticsearchMigrator\Exceptions\InvalidAliasNameException;

class Migration {

    protected $elasticsearch;
    protected $alias;
    protected $prefix;
    protected $version;
    protected $index;

    public function __construct(Client $elasticsearch, string $aliasName, string $prefix, array $index, int $version = null)
    {
        $this->elasticsearch = $elasticsearch;
        $this->aliasName = $aliasName;
        $this->prefix = $prefix;
        $this->index = $index;

        if (!is_null($version)) {
            $this->version = $version;
            $this->indexName = $this->getIndexName($this->version);
        }
    }

    public function getElasticsearchAliases()
    {
        return $this->elasticsearch->indices();
    }

    private function getIndexName($version, $prefix = null)
    {
        return ($prefix ?? $this->prefix) . $version;
    }

    public function execute(bool $reindex = true, $replacedVersion = null)
    {
        $takenNames = [];

        $existingAliases = $this->elasticsearch->indices()->getAliases();
        $indexRegexp = '/^' . $this->getIndexName('(\d+)', preg_quote($this->prefix, '/')) . '/';

        $aliasedIndexNames = [];

        foreach ($existingAliases as $existingIndexName => $aliasContainer) {
            $takenNames[] = $existingIndexName;
            $takenNames = array_merge($takenNames, array_keys($aliasContainer['aliases']));

            if (!empty($this->indexName) && $existingIndexName === $this->indexName) {
                throw new IndexAlreadyExistsException("Index {$existingIndexName} already exists.");
            }

            $existingIndexAliases = $aliasContainer['aliases'];
            if (array_key_exists($this->aliasName, $existingIndexAliases)) {
                $aliasedIndexNames[] = $existingIndexName;
            }
        }

        // Check if alias with provided name could be created
        $takenNames = array_unique($takenNames);
        if (in_array($this->aliasName, $takenNames)) {
            throw new InvalidAliasNameException("Unable to create the alias with name '{$this->aliasName}' because other index or alias with the same name already exists.");
        }

        $existingVersions = array_filter(
            array_map(function ($indexName) use ($indexRegexp) {
                preg_match($indexRegexp, $indexName, $results);
                return $results[1] ?? null;
            }, $aliasedIndexNames)
        );

        rsort($existingVersions);

        // Auto-increment the version if it's not set explicitly
        if (empty($this->version)) {
            $this->version = $existingVersions ? $existingVersions[0] + 1 : 1;
            $this->indexName = $this->getIndexName($this->version);
        }

        // If explicitly set, replace the existing index with the new one or auto-choose the replaced index
        if (!is_null($replacedVersion)) {
            $existingIndexName = $this->getIndexName($replacedVersion);
            if (!in_array($replacedVersion, $existingVersions)) {
                throw new IndexNotFoundException("Index {$existingIndexName} was not found.");
            }
        } else {
            $existingIndexName = $existingVersions ? $this->getIndexName($existingVersions[0]) : null;
        }

        if ($existingIndexName === $this->indexName) {
            throw new IndexAlreadyExistsException("Index {$existingIndexName} already exists.");
        }

        $responses = [];

        // Create the new index
        $responses['create_index'] = $this->elasticsearch->indices()->create([
            'index' => $this->indexName,
            'body' => $this->index
        ]);

        $aliasActions = [];

        // If a replaced index was found, reindex the data from it to the new index and remove the alias
        if ($existingIndexName) {
            if ($reindex) {
                $responses['reindex'] = $this->elasticsearch->reindex([
                    'body' => [
                        'source' => [ 'index' => $existingIndexName ],
                        'dest' => [
                            'index' => $this->indexName,
                            'version_type' => 'external'
                        ],
                    ]
                ]);
            }

            $aliasActions[] = [
                'remove' => [
                    'index' => $existingIndexName,
                    'alias' => $this->aliasName
                ]
            ];
        }

        // Create an alias for the new index
        $aliasActions[] = [
            'add' => [
                'index' => $this->indexName,
                'alias' => $this->aliasName
            ]
        ];

        $responses['update_aliases'] = $this->elasticsearch->indices()->updateAliases([
            'body' => [
                'actions' => $aliasActions
            ]
        ]);

        return [
            'replaced_index' => $existingIndexName,
            'alias' => $this->aliasName,
            'index' => $this->indexName,
            'version' => $this->version,
            'responses' => $responses
        ];
    }

    public static function migrate(Client $elasticsearch, string $aliasName, array $index, $version = null) {
        $instance = new static($elasticsearch, $aliasName, $aliasName . '__v', $index, $version);

        return $instance->execute();
    }
}