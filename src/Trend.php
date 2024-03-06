<?php

namespace Flowframe\Trend;

use Carbon\CarbonPeriod;
use Error;
use Flowframe\Trend\Adapters\MongoDBAdapter;
use Flowframe\Trend\Adapters\MySqlAdapter;
use Flowframe\Trend\Adapters\PgsqlAdapter;
use Flowframe\Trend\Adapters\SqliteAdapter;
use MongoDB\Laravel\Eloquent\Builder;
use Illuminate\Support\Carbon;
use Illuminate\Support\Collection;


class Trend
{
    public string $interval;

    public Carbon $start;

    public Carbon $end;

    public string $dateColumn = 'creation_date';

    public string $dateAlias = 'date';

    public function __construct(public Builder $builder)
    {
    }

    public static function query(Builder $builder): self
    {
        return new static($builder);
    }

    public static function model(string $model): self
    {
        return new static($model::query());
    }

    public function between($start, $end): self
    {
        $this->start = $start;
        $this->end = $end;

        return $this;
    }

    public function interval(string $interval): self
    {
        $this->interval = $interval;

        return $this;
    }

    public function perMinute(): self
    {
        return $this->interval('minute');
    }

    public function perHour(): self
    {
        return $this->interval('hour');
    }

    public function perDay(): self
    {
        return $this->interval('day');
    }

    public function perMonth(): self
    {
        return $this->interval('month');
    }

    public function perYear(): self
    {
        return $this->interval('year');
    }

    public function dateColumn(string $column): self
    {
        $this->dateColumn = $column;

        return $this;
    }

    public function dateAlias(string $alias): self
    {
        $this->dateAlias = $alias;

        return $this;
    }

    // public function aggregate(string $column, string $aggregate): Collection
    // {
    //     dd($column, $aggregate);

    //     $values = $this->builder
    //         ->toBase()
    //         ->selectRaw("
    //             {$this->getSqlDate()} as {$this->dateAlias},
    //             {$aggregate}({$column}) as aggregate
    //         ")
    //         ->whereBetween($this->dateColumn, [$this->start, $this->end])
    //         ->groupBy($this->dateAlias)
    //         ->orderBy($this->dateAlias)
    //         ->get();

    //     return $this->mapValuesToDates($values);
    // }

//     public function aggregate(string $column, string $aggregate): Collection
// {
//     // Definir la pipeline de agregación
//     $pipeline = [
//         [
//             '$match' => [
//                 $this->dateColumn => [
//                     '$gte' => $this->start,
//                     '$lte' => $this->end,
//                 ],
//             ],
//         ],
//         [
//             '$group' => [
//                 '_id' => [
//                     '$dateToString' => [
//                         'format' => '%Y-%m-%d', // Cambia este formato según tu intervalo
//                         'date' => '$' . $this->dateColumn,
//                     ],
//                 ],
//                 'aggregate' => [
//                     '$' . $aggregate => '$' . $column,
//                 ],
//             ],
//         ],
//         [
//             '$sort' => ['_id' => 1],
//         ],
//     ];



//     // Ejecutar la consulta de agregación
//     $results = $this->builder->raw(function($collection) use ($pipeline) {
//         return $collection->aggregate($pipeline);
//     });

//     // dd($results);

//     // Convertir los resultados a una colección de Laravel
//     $values = collect($results)->map(function ($result) {
//         return new TrendValue(
//             date: $result['_id'],
//             aggregate: $result['aggregate'],
//         );
//     });



//     return $this->mapValuesToDates($values);
// }

public function aggregate(string $column = '*', string $aggregate = 'count'): Collection
{
    // Define el formato de fecha según tu intervalo
    // $dateFormat = $this->getMongoDateFormat(); // Implementa esta función según tu necesidad

    // Construye la pipeline de agregación
    $pipeline = [
        [
            '$match' => [
                $this->dateColumn => [
                    '$gte' => $this->start->valueOf(),
                    '$lte' => $this->end->valueOf(),
                ],
            ],
        ],
        [
            '$group' => [
                '_id' => $this->getSqlDate(),
                // Cambia aquí dependiendo de si es conteo u otro tipo de agregación
                'aggregate' => $aggregate === 'count' ? ['$sum' => 1] : ['$' . strtolower($aggregate) => $column === '*' ? null : '$' . $column],
            ],
        ],
        [
            '$project' => [
                $this->dateAlias => '$_id',
                'aggregate' => 1,
                '_id' => 0, // Excluir el campo _id de MongoDB
            ],
        ],
        [
            '$sort' => [$this->dateAlias => 1],
        ],
    ];


    // Ejecuta la pipeline de agregación
    $results = $this->builder->raw(function ($collection) use ($pipeline) {
        return $collection->aggregate($pipeline);
    });


    // Convierte los resultados en una colección de Laravel
    $values = collect($results)->map(function ($result) {
        return new TrendValue(
            date: $result[$this->dateAlias],
            aggregate: $result['aggregate'],
        );
    });

    return $this->mapValuesToDates($values);
}


    public function average(string $column): Collection
    {
        return $this->aggregate($column, 'avg');
    }

    public function min(string $column): Collection
    {
        return $this->aggregate($column, 'min');
    }

    public function max(string $column): Collection
    {
        return $this->aggregate($column, 'max');
    }

    public function sum(string $column): Collection
    {
        return $this->aggregate($column, 'sum');
    }

    public function count(string $column = '*'): Collection
    {

        return $this->aggregate($column, 'count');
    }

    public function mapValuesToDates(Collection $values): Collection
    {
        $values = $values->map(fn ($value) => new TrendValue(
            date: $value->{$this->dateAlias},
            aggregate: $value->aggregate,
        ));

        $placeholders = $this->getDatePeriod()->map(
            fn (Carbon $date) => new TrendValue(
                date: $date->format($this->getCarbonDateFormat()),
                aggregate: 0,
            )
        );

        return $values
            ->merge($placeholders)
            ->unique('date')
            ->sort()
            ->flatten();
    }

    protected function getDatePeriod(): Collection
    {
        return collect(
            CarbonPeriod::between(
                $this->start,
                $this->end,
            )->interval("1 {$this->interval}")
        );
    }

    protected function getSqlDate()
    {
        $adapter = match ($this->builder->getConnection()->getDriverName()) {
            'mongodb' => new MongoDBAdapter(),
            'mysql' => new MySqlAdapter(),
            'sqlite' => new SqliteAdapter(),
            'pgsql' => new PgsqlAdapter(),
            default => throw new Error('Unsupported database driver.'),
        };

        return $adapter->format($this->dateColumn, $this->interval);
    }

    protected function getCarbonDateFormat(): string
    {
        return match ($this->interval) {
            'minute' => 'Y-m-d H:i:00',
            'hour' => 'Y-m-d H:00',
            'day' => 'Y-m-d',
            'month' => 'Y-m',
            'year' => 'Y',
            default => throw new Error('Invalid interval.'),
        };
    }
}
