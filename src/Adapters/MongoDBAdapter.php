<?php

namespace Flowframe\Trend\Adapters;

use Error;

class MongoDBAdapter extends AbstractAdapter
{
    public function format(string $column, string $interval): array
    {
        $format = match ($interval) {
            'minute' => '%Y-%m-%d %H:%M:00',
            'hour' => '%Y-%m-%d %H:00',
            'day' => '%Y-%m-%d',
            'month' => '%Y-%m',
            'year' => '%Y',
            default => throw new Error('Invalid interval.'),
        };


        // En MongoDB, el formato de fecha se realiza dentro de una operación de agregación,
        // así que devolvemos un array representando la parte de la pipeline de agregación necesaria para formatear la fecha.
        return [
            '$dateToString' => [
                'format' => $format,
                'date' => ['$toDate' => ['$multiply' => ['$' . $column , 1]]],
                // Opcionalmente, puedes agregar 'timezone' para convertir la fecha a una zona horaria específica
                // 'timezone' => 'America/New_York'
            ]
        ];
    }
}
