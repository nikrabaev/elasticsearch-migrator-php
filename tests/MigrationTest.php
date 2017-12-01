<?php
declare(strict_types=1);

use PHPUnit\Framework\TestCase;

final class MigrationTest extends TestCase
{
    protected $elasticsearch;

    public function testCanBePerformedWithUniqueAliasName(): void
    {
        $aliasName = $this->generateRandomString();
        $result = \ElasticsearchMigrator\Migration::migrate($this->getElasticsearchClient(), $aliasName, [
            'mappings' => [
                'users' => [
                    'properties' => [
                        'name' => [
                            'type' => 'text'
                        ]
                    ]
                ]
            ]
        ]);

        // Cleanup
        $this->getElasticsearchClient()->indices()->delete([
            'index' => $aliasName
        ]);

        $this->assertEquals(null, $result['replaced_index']);
        $this->assertEquals($aliasName, $result['alias']);
        $this->assertEquals($aliasName . '__v1', $result['index']);
        $this->assertEquals(1, $result['version']);
        $this->assertArrayHasKey('create_index', $result['responses']);
        $this->assertArrayHasKey('update_aliases', $result['responses']);
        $this->assertContainsOnly('array', $result['responses']);
    }

    public function testCanNotBePerformedWithNotUniqueAliasName() : void
    {
        $aliasName = $this->generateRandomString();

        $this->getElasticsearchClient()->indices()->create([
            'index' => $aliasName,
            'body' => [
                'mappings' => [
                    'cities' => [
                        'properties' => [
                            'name' => [
                                'type' => 'text'
                            ],
                            'country_code' => [
                                'type' => 'keyword'
                            ]
                        ]
                    ]
                ]
            ]
        ]);

        $this->expectException(\ElasticsearchMigrator\Exceptions\InvalidAliasNameException::class);

        \ElasticsearchMigrator\Migration::migrate($this->getElasticsearchClient(), $aliasName, [
            'mappings' => [
                'users' => [
                    'properties' => [
                        'name' => [
                            'type' => 'text'
                        ]
                    ]
                ]
            ]
        ]);

        // Cleanup
        $this->getElasticsearchClient()->indices()->delete([
            'index' => $aliasName
        ]);
    }

    private function getElasticsearchClient()
    {
        if (!isset($this->elasticsearch)) {
            $this->elasticsearch = Elasticsearch\ClientBuilder::fromConfig([
                'hosts' => [ getenv('ELASTICSEARCH_HOST') ]
            ]);
        }

        return $this->elasticsearch;
    }

    private function generateRandomString($length = 10) {
        $characters = '0123456789abcdefghijklmnopqrstuvwxyz';
        $charactersLength = strlen($characters);
        $randomString = '';
        for ($i = 0; $i < $length; $i++) {
            $randomString .= $characters[rand(0, $charactersLength - 1)];
        }
        return $randomString;
    }
}