<?php
namespace App\Jobs;

use App\Events\LogEvent;
use App\Models\Installment;
use App\Models\Location;
use App\Models\Log;
use App\Models\Payment;
use App\Models\User;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use Illuminate\Support\Facades\DB;
use Throwable;

class ImportPayments implements ShouldQueue
{
    use Queueable;

    private const REQUIRED_FIELDS = [
        'date',
        'document_number',
        'eligible_for_rebate',
        // 'generic_collector',
        'location',
        'maximum_of_amount',
        // 'memo_main',
        // 'minimum_invoice_number_leasing',
        'minimum_sales_order_number_for_dp',
        'name',
        'or_number',
    ];

    /**
     * Create a new job instance.
     */
    public function __construct(
        public string $batch,
        public array $data,
        public User $user,
        public string $file_name
    ) {
        //
    }

    /**
     * Execute the job.
     *
     * All lookups are prefetched once per batch, so the query count stays
     * constant (a handful) no matter how many rows are in the batch.
     */
    public function handle(): void
    {
        $rows = collect($this->data);

        // ---- Prefetch (3 queries instead of ~4 per row) ------------------
        $installments = $this->loadInstallments($rows);
        $locations    = $this->loadLocations($rows);
        $existing     = $this->loadExistingPayments($rows);

        $payments = [];
        $messages = []; // every message that becomes a Log row + LogEvent
        $now      = now();

        foreach ($rows as $item) {
            // 1. Required fields
            $dataMissing = array_values(array_filter(
                self::REQUIRED_FIELDS,
                fn ($field) => !isset($item[$field])
            ));

            if (!empty($dataMissing)) {
                $docNo  = $item['document_number'] ?? 'N/A';
                $fields = implode(', ', $dataMissing);

                $messages[] = "Unable to save payment for Document number : {$docNo} due to missing required fields: {$fields}.";
                continue;
            }

            // 2. Installment lookup (in memory)
            $installment = $installments[$this->key($this->soNumber($item))] ?? null;

            if (!$installment) {
                $messages[] = "Unable to save payment for Document No. {$item['document_number']}: No matching installment found for {$item['minimum_sales_order_number_for_dp']}.";
                continue;
            }

            // 3. Skip payments that already exist (or were already seen in this batch)
            $paymentKey = $this->key($item['document_number']) . '|' . $this->key($item['or_number']);

            if (isset($existing[$paymentKey])) {
                continue;
            }
            $existing[$paymentKey] = true;

            // 4. Location (only hits the DB the first time a new location is seen)
            $locationKey = $this->key($item['location']);
            $location = $locations[$locationKey] ??= Location::firstOrCreate(
                ['location_name' => $item['location']]
            );

            $payments[] = [
                'location_id'       => $location->id,
                'installment_id'    => $installment->id,
                'payment_date'      => $item['date'],
                'document_number'   => $item['document_number'],
                'or_number'         => $item['or_number'],
                'amount'            => $item['maximum_of_amount'],
                'customer_name'     => $item['name'],
                'isrebatable'       => in_array(
                    strtolower(trim((string) $item['eligible_for_rebate'])),
                    ['yes', '1'],
                    true
                ),
                'memo_description'  => $item['memo_main'] ?? null,
                'invoice_number'    => isset($item['minimum_invoice_number_leasing'])
                    ? trim(str_replace('Invoice #', '', $item['minimum_invoice_number_leasing']))
                    : null,
                'generic_collector' => $item['generic_collector'] ?? null,
                'created_at'        => $now,
                'updated_at'        => $now,
            ];
        }

        // ---- Writes: one transaction, chunked bulk inserts ---------------
        if (!empty($payments)) {
            DB::transaction(function () use ($payments) {
                foreach (array_chunk($payments, 500) as $chunk) {
                    Payment::insert($chunk);
                }
            });
        }

        // ---- Logs: one bulk insert, then events --------------------------
        $messages = collect($messages)->unique()->values()->all();

        if (!empty($messages)) {
            $logRows = array_map(fn ($info) => [
                'info'       => $info,
                'created_at' => $now,
                'updated_at' => $now,
            ], $messages);

            foreach (array_chunk($logRows, 500) as $chunk) {
                Log::insert($chunk);
            }

            foreach ($messages as $info) {
                $this->dispatchLogEvent($info);
            }
        }

        $info = sprintf(
            "File uploaded: %s \nBatch: %s successfully \nUploader: %s",
            $this->file_name,
            $this->batch,
            $this->user->name
        );

        Log::create([
            'file_name' => $this->file_name,
            'info'      => $info,
        ]);

        $this->dispatchLogEvent($info);
    }

    public function failed(?Throwable $exception): void
    {
        $info = sprintf(
            "File upload failed: %s\nBatch: %s failed\nUploader: %s\nError: %s",
            $this->file_name,
            $this->batch,
            $this->user->name,
            $exception?->getMessage() ?? 'Unknown error'
        );

        Log::create([
            'file_name' => $this->file_name,
            'info'      => $info,
        ]);

        $this->dispatchLogEvent($info);
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private function dispatchLogEvent(string $info): void
    {
        event(new LogEvent(
            $this->user->id,
            $this->file_name,
            $info,
            now()->toISOString()
        ));
    }

    /**
     * Normalised lookup key. Lower-case + trim mimics MySQL's default
     * case-insensitive collation, so in-memory matching agrees with the
     * old `where(...)` queries.
     */
    private function key(mixed $value): string
    {
        return mb_strtolower(trim((string) $value));
    }

    private function soNumber(array $item): string
    {
        return str_replace('Sales Order #', '', (string) ($item['minimum_sales_order_number_for_dp'] ?? ''));
    }

    /** @return \Illuminate\Support\Collection<string, Installment> keyed by normalised so_number */
    private function loadInstallments($rows)
    {
        $soNumbers = $rows
            ->map(fn ($item) => $this->soNumber($item))
            ->filter()
            ->unique()
            ->values();

        $found = collect();

        foreach ($soNumbers->chunk(1000) as $chunk) {
            $found = $found->concat(
                Installment::whereIn('so_number', $chunk->all())->get(['id', 'so_number'])
            );
        }

        return $found->keyBy(fn ($i) => $this->key($i->so_number));
    }

    /** @return array<string, Location> keyed by normalised location_name */
    private function loadLocations($rows): array
    {
        $names = $rows->pluck('location')->filter()->unique()->values();

        if ($names->isEmpty()) {
            return [];
        }

        return Location::whereIn('location_name', $names->all())
            ->get()
            ->keyBy(fn ($l) => $this->key($l->location_name))
            ->all();
    }

    /** @return array<string, true> set of "document|or" keys already in the DB */
    private function loadExistingPayments($rows): array
    {
        $docs = $rows->pluck('document_number')->filter()->unique()->values();
        $existing = [];

        foreach ($docs->chunk(1000) as $chunk) {
            $found = Payment::whereIn('document_number', $chunk->all())
                ->get(['document_number', 'or_number']);

            foreach ($found as $p) {
                $existing[$this->key($p->document_number) . '|' . $this->key($p->or_number)] = true;
            }
        }

        return $existing;
    }
}
