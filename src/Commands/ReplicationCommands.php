<?php

namespace Drupal\replication\Commands;

use Consolidation\AnnotatedCommand\CommandData;
use Consolidation\AnnotatedCommand\CommandError;
use Doctrine\CouchDB\CouchDBClient;
use Drush\Commands\DrushCommands;
use Drush\Exceptions\UserAbortException;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Extension\ModuleInstallerInterface;
use Relaxed\Replicator\ReplicationTask;
use Relaxed\Replicator\Replication;

/**
 * Drush commands for replication.
 */
class ReplicationCommands extends DrushCommands {

  /**
   * The entity type manager.
   *
   * @var \Drupal\Core\Entity\EntityTypeManagerInterface
   */
  protected $entityTypeManager;

  /**
   * The module installer.
   *
   * @var \Drupal\Core\Extension\ModuleInstallerInterface
   */
  protected $moduleInstaller;

  /**
   * Constructs a new ReplicationCommands object.
   *
   * @param \Drupal\Core\Entity\EntityTypeManagerInterface $entity_type_manager
   *   The entity type manager.
   * @param \Drupal\Core\Extension\ModuleInstallerInterface $module_installer
   *   The module installer.
   */
  public function __construct(EntityTypeManagerInterface $entity_type_manager, ModuleInstallerInterface $module_installer) {
    parent::__construct();

    $this->entityTypeManager = $entity_type_manager;
    $this->moduleInstaller = $module_installer;
  }

  /**
   * Uninstall Replication.
   *
   * @command replication:uninstall
   * @aliases repun,replication-uninstall
   */
  public function uninstall() {
    $extension = 'replication';
    $uninstall = TRUE;
    $extension_info = system_rebuild_module_data();

    $info = $extension_info[$extension]->info;
    if ($info['required']) {
      $explanation = '';
      if (!empty($info['explanation'])) {
        $explanation = ' ' . dt('Reason: !explanation.', [
          '!explanation' => strip_tags($info['explanation']),
        ]);
      }
      $this->logger()->info(dt('!extension is a required extension and can\'t be uninstalled.', [
        '!extension' => $extension,
      ]) . $explanation);
      $uninstall = FALSE;
    }
    elseif (!$extension_info[$extension]->status) {
      $this->logger()->info(dt('!extension is already uninstalled.', [
        '!extension' => $extension,
      ]));
      $uninstall = FALSE;
    }
    elseif ($extension_info[$extension]->getType() == 'module') {
      $dependents = [];
      foreach (array_keys($extension_info[$extension]->required_by) as $dependent) {
        $dependent_info = $extension_info[$dependent];
        if (!$dependent_info->required && $dependent_info->status) {
          $dependents[] = $dependent;
        }
      }
      if (count($dependents)) {
        $this->logger()->error(dt('To uninstall !extension, the following extensions must be uninstalled first: !required', [
          '!extension' => $extension,
          '!required' => implode(', ', $dependents),
        ]));
        $uninstall = FALSE;
      }
    }

    if ($uninstall) {
      $this->output()->writeln(dt('Replication will be uninstalled.'));
      if (!$this->io()->confirm(dt('Do you really want to continue?'))) {
        throw new UserAbortException();
      }

      try {
        // Delete all replication_log entities.
        $storage = $this->entityTypeManager->getStorage('replication_log')->getOriginalStorage();
        $entities = $storage->loadMultiple();
        $storage->delete($entities);

        $this->moduleInstaller->uninstall([$extension]);
      }
      catch (Exception $e) {
        $this->logger()->error($e->getMessage());
      }

      // Inform the user of final status.
      $this->logger()->info(dt('!extension was successfully uninstalled.', [
        '!extension' => $extension,
      ]));
    }
  }

  /**
   * Start a replication.
   *
   * @param string $source
   *   Source database.
   * @param string $target
   *   Target database.
   * @param array $options
   *   An associative array of options whose values come from cli, aliases,
   *   config, etc.
   *
   * @option continuous
   *   Continuous replication.
   * @option replicator
   *   The used replicator.
   *
   * @command replication:start
   * @aliases replication-start
   * @validate-replication-endpoints
   */
  public function start($source, $target, array $options = ['continuous' => NULL, 'replicator' => NULL]) {
    try {
      $source_client = $this->getCouchDbClient($source);
      $target_client = $this->getCouchDbClient($target);
      // Create the replication task.
      $task = new ReplicationTask();
      // Create the replication.
      $replication = new Replication($source_client, $target_client, $task);
      // Generate and set a replication ID.
      $replication->task->setRepId($replication->generateReplicationId());
      // Start the replication.
      $replicationResult = $replication->start();
      return $replicationResult;
    }
    catch (\Exception $e) {
      $this->logger()->error($e->getMessage());
    }
  }

  /**
   * Stop a replication.
   *
   * @param string $source
   *   Source database.
   * @param string $target
   *   Target database.
   * @param array $options
   *   An associative array of options whose values come from cli, aliases,
   *   config, etc.
   *
   * @option continuous
   *   Continuous replication.
   * @option replicator
   *   The used replicator.
   *
   * @command replication:stop
   * @aliases replication-stop
   * @validate-replication-endpoints
   */
  public function stop($source, $target, array $options = ['continuous' => NULL, 'replicator' => NULL]) {
    try {
      $client = $this->getCouchDbClient();
      $continuous = $options['continuous'];
      return $client->replicate($source, $target, TRUE, $continuous);
    }
    catch (\Exception $e) {
      $this->logger()->error($e->getMessage());
    }
  }

  /**
   * Returns a list of active replication tasks between databases.
   *
   * @param string $source
   *   Source database.
   * @param string $target
   *   Target database.
   * @param array $options
   *   An associative array of options whose values come from cli, aliases,
   *   config, etc.
   *
   * @option replicator
   *   The used replicator.
   *
   * @command replication:active
   * @aliases replication-active
   * @validate-replication-endpoints
   */
  public function active($source, $target, array $options = ['replicator' => NULL]) {
    try {
      $client = $this->getCouchDbClient();
      $results = $client->getActiveTasks();
      foreach ($results as $key => $result) {
        $results[$key]['started_on'] = date('D, j M Y, H:i:s e', $result['started_on']);
        if ($source && $target && is_array($results)) {
          $source_diff = array_diff(($result['source']), $this->getUrlParts($source));
          $target_diff = array_diff($this->getUrlParts($result['target']), $this->getUrlParts($target));
          if (empty($source_diff) && empty($target_diff)) {
            // Return information about one active replication.
            return [$results[$key]];
          }
          else {
            $this->output()->writeln('No active replication.');
            return;
          }
        }
      }
      if (!empty($results)) {
        // Return information about all active replications.
        return $results;
      }
      else {
        $this->output()->writeln('No active replications.');
      }
    }
    catch (\Exception $e) {
      $this->logger()->error($e->getMessage());
    }
  }

  /**
   * Helper function for command validation.
   *
   * @hook validate @validate-replication-endpoints
   */
  public function validateReplicationEndpoints(CommandData $commandData) {
    $invalid = [];
    foreach (['source', 'target'] as $endpoint_type) {
      $url = $commandData->input()->getArgument($endpoint_type);
      if ($this->getResponseCode($url) != 200) {
        $invalid[] = $url;
      }
    }

    if (count($invalid)) {
      return new CommandError(dt('Database(s) not found: !dbs', [
        '!dbs' => implode(', ', $invalid),
      ]));
    }
  }

  /**
   * Helper function to retrieve the http response code.
   */
  public function getResponseCode($url) {
    $ch = curl_init($url);
    curl_setopt($ch, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
    curl_exec($ch);
    $httpcode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);
    return $httpcode;
  }

  /**
   * Helper function to initiate a CouchDB client connection.
   *
   * @param string $url
   *   The URL to use.
   *
   * @return \Doctrine\CouchDB\CouchDBClient
   *   A CouchDBClient connection object.
   */
  public function getCouchDbClient($url = '') {
    return CouchDBClient::create([
      'url' => (string) $url,
      'timeout' => 10,
    ]);
  }

  /**
   * Returns url parts (host, port, path, user and pass).
   *
   * @param string $url
   *   The URL to process.
   * @param bool $credentials
   *   Flag to indicate if credentials should be parsed.
   *
   * @return array
   *   An associative array consisting of the different path components.
   */
  public function getUrlParts($url, $credentials = FALSE) {
    $url_parts = parse_url($url);
    $options = [
      'host' => $url_parts['host'],
      'port' => $url_parts['port'],
    ];
    $path = trim($url_parts['path'], '/');
    if ($path != '') {
      $options['path'] = $path;
    }
    if ($credentials) {
      $options['user'] = $url_parts['user'] ? $url_parts['user'] : NULL;
      $options['password'] = $url_parts['pass'] ? $url_parts['pass'] : NULL;
    }
    return $options;
  }

}
